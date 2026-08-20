//! Sled-backed Raft log storage.
//!
//! [`SledLogStore`] persists Raft log entries and vote metadata using
//! [sled](https://docs.rs/sled) — an embedded, ordered key-value store.
//!
//! Two named trees are used:
//!
//! - **`log`** — log entries keyed by big-endian `u64` index so that sled's
//!   ordered iteration yields entries in index order.
//! - **`meta`** — small metadata values: `vote`, `committed`, and
//!   `last_purged`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::storage::LogFlushed;
use openraft::{
    AnyError, Entry, LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    FerrosRaftConfig, IndexNodeStatus, NodeInfo, NodeState, RaftCommand, RaftOp, RaftResponse,
    Token,
};
use crate::config::ClusterConfig;
use ferrosa_common::CqlType;
use ferrosa_schema::metadata::aggregate::UserAggregateMetadata;
use ferrosa_schema::metadata::function::UserFunctionMetadata;
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, KeyspaceUpdates};
use ferrosa_schema::metadata::table::{TableMetadata, TableUpdates};
use ferrosa_schema::metadata::user_type::UserTypeMetadata;
use ferrosa_schema::{GrantEntry, Permission, Resource, RoleMetadata, RoleUpdates};

openraft::declare_raft_types!(
    /// Legacy Raft type configuration for entries written before UpdateNodeInfo
    /// was inserted into the middle of `RaftOp`.
    #[allow(dead_code)]
    LegacyFerrosRaftConfigPreUpdateNodeInfo:
        D            = LegacyRaftCommandPreUpdateNodeInfo,
        R            = RaftResponse,
        NodeId       = u64,
        Node         = openraft::BasicNode,
        Entry        = openraft::Entry<LegacyFerrosRaftConfigPreUpdateNodeInfo>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyRaftCommandPreUpdateNodeInfo {
    op: LegacyRaftOpPreUpdateNodeInfo,
    schema_version: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LegacyRaftOpPreUpdateNodeInfo {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(Box<TableMetadata>),
    DropTable {
        keyspace: String,
        table: String,
    },
    AlterKeyspace {
        name: String,
        updates: KeyspaceUpdates,
    },
    AlterTable {
        keyspace: String,
        table: String,
        updates: Box<TableUpdates>,
    },
    CreateRole(RoleMetadata),
    AlterRole {
        name: String,
        updates: RoleUpdates,
    },
    DropRole(String),
    Grant(GrantEntry),
    Revoke {
        role: String,
        resource: Resource,
        permission: Permission,
    },
    CreateIndex(IndexMetadata),
    DropIndex {
        keyspace: String,
        table: String,
        index: String,
    },
    IndexStatus {
        node_id: u64,
        keyspace: String,
        table: String,
        index_name: String,
        status: IndexNodeStatus,
    },
    CreateType(UserTypeMetadata),
    DropType {
        keyspace: String,
        name: String,
    },
    CreateFunction(UserFunctionMetadata),
    DropFunction {
        keyspace: String,
        name: String,
        arg_types: Vec<CqlType>,
    },
    CreateAggregate(UserAggregateMetadata),
    DropAggregate {
        keyspace: String,
        name: String,
        arg_types: Vec<CqlType>,
    },
    JoinNode(NodeInfo),
    LeaveNode {
        node_id: u64,
    },
    AssignTokens {
        node_id: u64,
        tokens: Vec<Token>,
    },
    UpdateConfig(ClusterConfig),
    ApproveNode {
        host_id: Uuid,
    },
    SetNodeState {
        node_id: u64,
        state: NodeState,
    },
}

impl From<LegacyRaftOpPreUpdateNodeInfo> for RaftOp {
    fn from(value: LegacyRaftOpPreUpdateNodeInfo) -> Self {
        match value {
            LegacyRaftOpPreUpdateNodeInfo::CreateKeyspace(ks) => Self::CreateKeyspace(ks),
            LegacyRaftOpPreUpdateNodeInfo::DropKeyspace(name) => Self::DropKeyspace(name),
            LegacyRaftOpPreUpdateNodeInfo::CreateTable(table) => Self::CreateTable(table),
            LegacyRaftOpPreUpdateNodeInfo::DropTable { keyspace, table } => {
                Self::DropTable { keyspace, table }
            }
            LegacyRaftOpPreUpdateNodeInfo::AlterKeyspace { name, updates } => {
                Self::AlterKeyspace { name, updates }
            }
            LegacyRaftOpPreUpdateNodeInfo::AlterTable {
                keyspace,
                table,
                updates,
            } => Self::AlterTable {
                keyspace,
                table,
                updates,
            },
            LegacyRaftOpPreUpdateNodeInfo::CreateRole(role) => Self::CreateRole(role),
            LegacyRaftOpPreUpdateNodeInfo::AlterRole { name, updates } => {
                Self::AlterRole { name, updates }
            }
            LegacyRaftOpPreUpdateNodeInfo::DropRole(name) => Self::DropRole(name),
            LegacyRaftOpPreUpdateNodeInfo::Grant(entry) => Self::Grant(entry),
            LegacyRaftOpPreUpdateNodeInfo::Revoke {
                role,
                resource,
                permission,
            } => Self::Revoke {
                role,
                resource,
                permission,
            },
            LegacyRaftOpPreUpdateNodeInfo::CreateIndex(index) => Self::CreateIndex(index),
            LegacyRaftOpPreUpdateNodeInfo::DropIndex {
                keyspace,
                table,
                index,
            } => Self::DropIndex {
                keyspace,
                table,
                index,
            },
            LegacyRaftOpPreUpdateNodeInfo::IndexStatus {
                node_id,
                keyspace,
                table,
                index_name,
                status,
            } => Self::IndexStatus {
                node_id,
                keyspace,
                table,
                index_name,
                status,
            },
            LegacyRaftOpPreUpdateNodeInfo::CreateType(udt) => Self::CreateType(udt),
            LegacyRaftOpPreUpdateNodeInfo::DropType { keyspace, name } => {
                Self::DropType { keyspace, name }
            }
            LegacyRaftOpPreUpdateNodeInfo::CreateFunction(func) => Self::CreateFunction(func),
            LegacyRaftOpPreUpdateNodeInfo::DropFunction {
                keyspace,
                name,
                arg_types,
            } => Self::DropFunction {
                keyspace,
                name,
                arg_types,
            },
            LegacyRaftOpPreUpdateNodeInfo::CreateAggregate(agg) => Self::CreateAggregate(agg),
            LegacyRaftOpPreUpdateNodeInfo::DropAggregate {
                keyspace,
                name,
                arg_types,
            } => Self::DropAggregate {
                keyspace,
                name,
                arg_types,
            },
            LegacyRaftOpPreUpdateNodeInfo::JoinNode(node) => Self::JoinNode(node),
            LegacyRaftOpPreUpdateNodeInfo::LeaveNode { node_id } => Self::LeaveNode { node_id },
            LegacyRaftOpPreUpdateNodeInfo::AssignTokens { node_id, tokens } => {
                Self::AssignTokens { node_id, tokens }
            }
            LegacyRaftOpPreUpdateNodeInfo::UpdateConfig(config) => Self::UpdateConfig(config),
            LegacyRaftOpPreUpdateNodeInfo::ApproveNode { host_id } => Self::ApproveNode { host_id },
            LegacyRaftOpPreUpdateNodeInfo::SetNodeState { node_id, state } => {
                Self::SetNodeState { node_id, state }
            }
        }
    }
}

fn convert_legacy_entry_pre_update_node_info(
    entry: openraft::Entry<LegacyFerrosRaftConfigPreUpdateNodeInfo>,
) -> Entry<FerrosRaftConfig> {
    let payload = match entry.payload {
        openraft::EntryPayload::Blank => openraft::EntryPayload::Blank,
        openraft::EntryPayload::Membership(membership) => {
            openraft::EntryPayload::Membership(membership)
        }
        openraft::EntryPayload::Normal(cmd) => openraft::EntryPayload::Normal(RaftCommand {
            op: cmd.op.into(),
            schema_version: cmd.schema_version,
        }),
    };
    Entry {
        log_id: entry.log_id,
        payload,
    }
}

// ---------------------------------------------------------------------------
// Meta-tree keys
// ---------------------------------------------------------------------------

const META_VOTE: &[u8] = b"vote";
const META_COMMITTED: &[u8] = b"committed";
const META_LAST_PURGED: &[u8] = b"last_purged";

#[derive(Debug, Default, Clone)]
pub struct RecoveredTopology {
    pub members: BTreeMap<u64, NodeInfo>,
    pub token_map: BTreeMap<Token, u64>,
}

// ---------------------------------------------------------------------------
// SledLogStore
// ---------------------------------------------------------------------------

/// Sled-backed implementation of openraft's `RaftLogStorage` and
/// `RaftLogReader` traits.
pub struct SledLogStore {
    #[allow(dead_code)]
    db: sled::Db,
    /// Tree for log entries: key = big-endian u64 index, value = bincode(Entry).
    log: sled::Tree,
    /// Tree for metadata: vote, committed, last_purged.
    meta: sled::Tree,
    /// Highest log index the state machine has *durably* persisted, or 0 when
    /// nothing is known to be durable yet.
    ///
    /// Shared with the state machine, which raises it only after a snapshot
    /// has been fsynced. `purge` will not delete past it: entries above this
    /// index exist nowhere else on this node, so deleting them is
    /// unrecoverable. See `crate::raft::local_state::purge_ceiling`.
    durable_applied: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

/// Counts of entries cleared by [`SledLogStore::reset`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetCounts {
    /// Entries removed from the `log` tree, when counted.
    ///
    /// Atomic directory replacement does not open sled, so normal reset
    /// reports `None` here rather than taking sled's exclusive directory lock.
    pub log_entries: Option<u64>,
    /// Keys removed from the `meta` tree (vote / committed / last_purged), when counted.
    pub meta_keys: Option<u64>,
    /// Previous raft directory retained for rollback/debugging.
    pub backup_path: Option<PathBuf>,
}

/// A persisted log entry whose bytes no decode path understands —
/// written by a build with an incompatible wire layout.
///
/// The Display text is what reaches the operator inside openraft's
/// `Fatal` at startup, so it names the index, the blast radius, and the
/// recovery tooling instead of a bare bincode message.
#[derive(Debug)]
struct UnreadableLogEntry {
    index: Option<u64>,
    detail: String,
}

impl std::fmt::Display for UnreadableLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.index {
            Some(index) => write!(f, "raft log entry {index} is unreadable")?,
            None => f.write_str("a raft log entry with a malformed key is unreadable")?,
        }
        write!(
            f,
            " ({}): the entry was written by a build with an incompatible wire \
             layout. The metadata plane is down (no DDL replication or membership \
             changes; /readyz reports not-ready) while CQL continues to serve. \
             Recover: stop this node, run `ferrosa-ctl raft log-inspect --data-dir \
             <raft-dir>` to map the damage, then `ferrosa-ctl raft log-truncate \
             --data-dir <raft-dir> --from <first-bad-index>` to drop the unreadable \
             tail (or `ferrosa-ctl raft reset` to resync everything from the \
             leader). See specs/implemented/bug-raft-log-bincode-format-instability.md",
            self.detail
        )
    }
}

impl std::error::Error for UnreadableLogEntry {}

/// One undecodable entry found by [`SledLogStore::inspect`].
#[derive(Debug, Clone, Serialize)]
pub struct UndecodableEntry {
    /// Log index (from the sled key).
    pub index: u64,
    /// Decode error text.
    pub error: String,
    /// Hex of the first bytes of the on-disk value, for drift forensics.
    pub preview_hex: String,
}

/// Offline report over a node's persisted raft log
/// (`ferrosa-ctl raft log-inspect`).
#[derive(Debug, Clone, Serialize)]
pub struct LogInspection {
    /// Persisted vote (term, voted-for), if any.
    pub vote: Option<Vote<u64>>,
    /// Persisted committed marker, if any.
    pub committed: Option<LogId<u64>>,
    /// Purge point — entries at or below this are snapshot-covered.
    pub last_purged: Option<LogId<u64>>,
    /// Entries present in the log tree.
    pub total_entries: u64,
    /// Entries that decode with the current build.
    pub decoded_entries: u64,
    /// Entries no decode path understands.
    pub undecodable_count: u64,
    /// Lowest index present.
    pub first_index: Option<u64>,
    /// Highest index present.
    pub last_index: Option<u64>,
    /// The lowest-index undecodable entry — the `--from` for log-truncate.
    pub first_undecodable: Option<UndecodableEntry>,
}

/// Outcome of [`SledLogStore::truncate_from`]
/// (`ferrosa-ctl raft log-truncate`).
#[derive(Debug, Clone, Serialize)]
pub struct TruncateReport {
    /// Entries removed.
    pub removed_entries: u64,
    /// Lowest removed index.
    pub first_removed_index: Option<u64>,
    /// Highest index remaining in the log, if any.
    pub new_last_index: Option<u64>,
    /// Committed marker before truncation.
    pub committed_before: Option<LogId<u64>>,
    /// Committed marker after clamping to the surviving log/purge point.
    pub committed_after: Option<LogId<u64>>,
}

#[allow(clippy::result_large_err)] // StorageIOError is 224 bytes — dictated by openraft
impl SledLogStore {
    /// Open (or create) a log store at the given filesystem `path`.
    ///
    /// Retries briefly through **transient** directory-lock contention. sled
    /// takes an exclusive `flock` on the data dir and surfaces contention as an
    /// `Io` "could not acquire lock" error (`EWOULDBLOCK`/`EAGAIN`). That fires
    /// on a millisecond-scale race — a just-exited handle still releasing, or
    /// (seen in CI) heavy parallel I/O making `flock` momentarily return
    /// `Resource temporarily unavailable` even on a fresh dir. Failing the open
    /// on such a transient is wrong, so we retry with a bounded backoff
    /// (≤ `MAX_ATTEMPTS` × `BACKOFF` ≈ 500 ms). A **genuinely** held lock (a live
    /// node already running on this dir) is held for the node's whole lifetime,
    /// far longer than the budget, so a real dual-open conflict still surfaces
    /// the error — only the transient is absorbed.
    pub fn new(path: &Path) -> Result<Self, sled::Error> {
        let db = Self::open_sled_db_with_lock_retry(path)?;
        let log = db.open_tree("log")?;
        let meta = db.open_tree("meta")?;
        Ok(Self {
            db,
            log,
            meta,
            durable_applied: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Share this store's durable-applied watermark with the state machine.
    ///
    /// The state machine raises it after each fsynced snapshot; this store
    /// reads it to bound `purge`. They must be the same cell, which is why it
    /// is handed over at construction rather than passed per call -- openraft
    /// owns the two halves separately once Raft is running.
    pub fn durable_applied_handle(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        self.durable_applied.clone()
    }

    /// Open a sled `Db` at `path`, retrying through **transient** directory-lock
    /// contention with a bounded backoff (≤ `MAX_ATTEMPTS` × `BACKOFF` ≈ 500 ms,
    /// logged per attempt, classified by `is_lock_contention`).
    ///
    /// The single transient-lock retry primitive: used by [`Self::new`] and the
    /// offline tooling, and exposed (`pub`) for tools/tests that need the raw
    /// `sled::Db` directly (manipulating trees) and would otherwise hit the same
    /// `EWOULDBLOCK`/`EAGAIN` flake on a fresh dir under heavy parallel I/O. A
    /// genuinely-held lock (a live node on this dir) outlasts the budget, so a
    /// real dual-open conflict still surfaces the error — only the transient is
    /// absorbed.
    pub fn open_sled_db_with_lock_retry(path: &Path) -> Result<sled::Db, sled::Error> {
        const MAX_ATTEMPTS: u32 = 10;
        const BACKOFF: Duration = Duration::from_millis(50);
        let mut attempt: u32 = 1;
        loop {
            match sled::open(path) {
                Ok(db) => return Ok(db),
                Err(err) if attempt < MAX_ATTEMPTS && Self::is_lock_contention(&err) => {
                    tracing::warn!(
                        path = %path.display(),
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        error = %err,
                        "sled open hit a transient directory lock; retrying after backoff"
                    );
                    std::thread::sleep(BACKOFF);
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Open a store for **offline** operator tooling (`inspect` /
    /// `truncate_from`). Same transient-lock retry as [`Self::new`]; kept as a
    /// named entry point for the offline tools.
    fn open_offline(path: &Path) -> Result<Self, sled::Error> {
        Self::new(path)
    }

    /// True only for the transient "directory lock already held" condition,
    /// not a real corruption/IO fault. sled wraps the OS `EWOULDBLOCK` as
    /// `Error::Io` whose message contains "could not acquire lock".
    fn is_lock_contention(err: &sled::Error) -> bool {
        match err {
            sled::Error::Io(io) => {
                io.kind() == std::io::ErrorKind::WouldBlock
                    || io.to_string().contains("could not acquire lock")
            }
            _ => false,
        }
    }

    /// Internal helper for `append` that does the actual sled work. Split
    /// out so the public `append` can route both success and failure
    /// through `LogFlushed::log_io_completed` (W1.19a). Test code can
    /// also exercise the error path here directly without needing to
    /// construct a `LogFlushed` callback (which has a `pub(crate)`
    /// constructor inside openraft).
    async fn append_inner<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<FerrosRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut batch = sled::Batch::default();
        for entry in entries {
            let key = Self::index_key(entry.log_id.index);
            let val = Self::serialize_entry(&entry)?;
            batch.insert(&key, val);
        }

        // Run sled disk IO on a blocking thread so the async Raft runtime
        // stays responsive for heartbeat processing.
        let log = self.log.clone();
        ferrosa_net::task_pool::TaskPool::current("raft-log-store")
            .spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
                log.apply_batch(batch)
                    .map_err(|e| Box::new(StorageIOError::write_logs(to_any_error(e))))?;
                Ok(())
            })
            .await
            .map_err(|e| StorageIOError::write_logs(to_any_error(e)))?
            .map_err(|e| *e)?;

        Ok(())
    }

    /// Wipe a stopped node's persisted Raft state at `path`.
    ///
    /// Atomically renames the existing raft directory aside, recreates an
    /// empty directory at the original path, and returns the backup path. This
    /// deliberately does not open sled, so reset itself does not acquire sled's
    /// exclusive database lock and callers can immediately reopen a fresh store.
    ///
    /// # Safety
    ///
    /// The node must be stopped before reset. Do not run this against a live
    /// process: POSIX filesystems may allow renaming a directory that another
    /// process still has open.
    ///
    /// Only uncommitted writes — those Raft never replicated to a quorum —
    /// can be lost. By Raft's durability guarantee, committed writes survive
    /// on the remaining majority and are replayed back to this node.
    pub fn reset(path: &Path) -> Result<ResetCounts, sled::Error> {
        let backup_path = if path.exists() {
            let backup = Self::unique_reset_backup_path(path);
            fs::rename(path, &backup).map_err(sled::Error::Io)?;
            if let Err(err) = fs::create_dir_all(path) {
                let _ = fs::rename(&backup, path);
                return Err(sled::Error::Io(err));
            }
            Some(backup)
        } else {
            fs::create_dir_all(path).map_err(sled::Error::Io)?;
            None
        };

        Ok(ResetCounts {
            log_entries: None,
            meta_keys: None,
            backup_path,
        })
    }

    /// Offline scan of a stopped node's raft log: decode every entry with
    /// the current build and report what is readable, what is not, and
    /// where the damage starts.
    ///
    /// Opens sled via `open_offline`: a brief transient lock (a just-exited
    /// node still releasing the directory lock) is retried, but a genuinely
    /// held lock — the node is still running — surfaces loudly. Never inspect
    /// a live store.
    pub fn inspect(path: &Path) -> Result<LogInspection, Box<dyn std::error::Error + Send + Sync>> {
        let store = Self::open_offline(path)?;

        let vote = Self::load_meta::<Vote<u64>>(&store.meta, META_VOTE)?;
        let committed =
            Self::load_meta::<Option<LogId<u64>>>(&store.meta, META_COMMITTED)?.unwrap_or(None);
        let last_purged = Self::load_meta::<LogId<u64>>(&store.meta, META_LAST_PURGED)?;

        let mut report = LogInspection {
            vote,
            committed,
            last_purged,
            total_entries: 0,
            decoded_entries: 0,
            undecodable_count: 0,
            first_index: None,
            last_index: None,
            first_undecodable: None,
        };

        for item in store.log.iter() {
            let (k, v) = item?;
            let index = Self::index_from_key(&k)
                .ok_or_else(|| format!("malformed log key ({} bytes; expected 8)", k.len()))?;
            report.total_entries += 1;
            report.first_index.get_or_insert(index);
            report.last_index = Some(index);

            match Self::deserialize_entry(&v) {
                Ok(_) => report.decoded_entries += 1,
                Err(e) => {
                    report.undecodable_count += 1;
                    if report.first_undecodable.is_none() {
                        report.first_undecodable = Some(UndecodableEntry {
                            index,
                            error: e.to_string(),
                            preview_hex: hex_preview(&v, 16),
                        });
                    }
                }
            }
        }

        Ok(report)
    }

    /// Offline removal of all log entries with index >= `from_index` from a
    /// stopped node's raft log, clamping the committed marker to the
    /// surviving log (or the purge point if the log empties).
    ///
    /// This is the recovery path for format-drift damage: the snapshot
    /// covers state through `last_purged`, the unreadable tail is
    /// discarded, and the cluster re-forms from the snapshot. Entries
    /// removed here that were committed are lost from the metadata plane —
    /// callers (ferrosa-ctl) must require explicit operator confirmation.
    pub fn truncate_from(
        path: &Path,
        from_index: u64,
    ) -> Result<TruncateReport, Box<dyn std::error::Error + Send + Sync>> {
        let store = Self::open_offline(path)?;

        let committed_before = Self::load_meta::<Option<LogId<u64>>>(&store.meta, META_COMMITTED)
            .map_err(|e| format!("reading committed marker: {e}"))?
            .unwrap_or(None);
        let last_purged = Self::load_meta::<LogId<u64>>(&store.meta, META_LAST_PURGED)
            .map_err(|e| format!("reading last_purged marker: {e}"))?;

        // Collect doomed keys first so the report is exact.
        let mut doomed = Vec::new();
        for item in store.log.range(Self::index_key(from_index)..) {
            let (k, _v) = item?;
            doomed.push(k);
        }
        let removed_entries = doomed.len() as u64;
        let first_removed_index = doomed.first().and_then(|k| Self::index_from_key(k));

        let mut batch = sled::Batch::default();
        for key in doomed {
            batch.remove(key);
        }
        store.log.apply_batch(batch)?;

        // The committed marker must not point past the surviving log: openraft
        // would otherwise wait on entries that no longer exist. Clamp to the
        // new last entry, falling back to the snapshot-covered purge point.
        let new_last = match store.log.last()? {
            Some((k, v)) => Some(Self::deserialize_entry_at(&k, &v).map_err(|e| {
                format!(
                    "entry below the truncation point is also unreadable — rerun \
                     log-inspect and truncate from the first bad index: {e}"
                )
            })?),
            None => None,
        };
        let new_last_log_id = new_last.map(|e| e.log_id);
        let new_last_index = new_last_log_id.map(|id| id.index);

        let committed_after = match committed_before {
            Some(c) if c.index >= from_index => new_last_log_id.or(last_purged),
            other => other,
        };
        Self::save_meta(&store.meta, META_COMMITTED, &committed_after)?;
        store.db.flush()?;

        Ok(TruncateReport {
            removed_entries,
            first_removed_index,
            new_last_index,
            committed_before,
            committed_after,
        })
    }

    // -- helpers ----------------------------------------------------------

    fn unique_reset_backup_path(path: &Path) -> PathBuf {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("raft");
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        for attempt in 0..u32::MAX {
            let candidate = parent.join(format!("{name}.reset-{pid}-{nanos}-{attempt}"));
            if !candidate.exists() {
                return candidate;
            }
        }

        parent.join(format!("{name}.reset-{pid}-{nanos}"))
    }

    fn index_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Recover the entry index from a log-tree key (big-endian u64).
    fn index_from_key(key: &[u8]) -> Option<u64> {
        key.try_into().ok().map(u64::from_be_bytes)
    }

    /// Magic prefix that disambiguates log-entry framings (W1.19c).
    ///
    /// Legacy entries (pre-Sprint-1) are bare bincoded `Entry`s that
    /// start with a `LogId.term: u64` little-endian byte.  A bincoded
    /// term value of `0x46` (chr 'F') is theoretically possible but
    /// vanishingly unlikely in steady state — and the second byte of
    /// 'F' would have to be 'R' (`0x52`), then 'E' (`0x45`), which
    /// would require term equal to `0x3145_5246` (little-endian
    /// `0x31 0x45 0x52 0x46`), a value above 800 million.  No real
    /// cluster reaches this.  We use the four magic bytes as a
    /// definitive "current-format" marker.
    const ENTRY_MAGIC: [u8; 4] = *b"FRE1";

    /// Tag for the current entry format (after the magic).  Bumping
    /// this signals a forward-compatible payload schema change.
    const ENTRY_FORMAT_VERSION: u8 = 1;

    fn serialize_entry(entry: &Entry<FerrosRaftConfig>) -> Result<Vec<u8>, StorageIOError<u64>> {
        let payload =
            bincode::serialize(entry).map_err(|e| StorageIOError::write_logs(to_any_error(e)))?;
        let mut out = Vec::with_capacity(payload.len() + 5);
        out.extend_from_slice(&Self::ENTRY_MAGIC);
        out.push(Self::ENTRY_FORMAT_VERSION);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn deserialize_entry(bytes: &[u8]) -> Result<Entry<FerrosRaftConfig>, StorageIOError<u64>> {
        // Tagged path: bytes start with FRE1 + version byte.
        if bytes.len() >= 5 && bytes[..4] == Self::ENTRY_MAGIC {
            let version = bytes[4];
            if version != Self::ENTRY_FORMAT_VERSION {
                // Unknown format version — fail loud.
                #[derive(Debug)]
                struct UnsupportedVersion(String);
                impl std::fmt::Display for UnsupportedVersion {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str(&self.0)
                    }
                }
                impl std::error::Error for UnsupportedVersion {}
                return Err(StorageIOError::read_logs(to_any_error(UnsupportedVersion(
                    format!(
                        "unsupported entry format version {version}; expected {}",
                        Self::ENTRY_FORMAT_VERSION
                    ),
                ))));
            }
            return bincode::deserialize(&bytes[5..])
                .map_err(|e| StorageIOError::read_logs(to_any_error(e)));
        }

        // Legacy path: bare bincoded Entry, with a fall-through to the
        // pre-UpdateNodeInfo schema for entries written by older
        // ferrosa builds.
        match bincode::deserialize(bytes) {
            Ok(entry) => Ok(entry),
            Err(current_err) => {
                match bincode::deserialize::<openraft::Entry<LegacyFerrosRaftConfigPreUpdateNodeInfo>>(
                    bytes,
                ) {
                    Ok(entry) => Ok(convert_legacy_entry_pre_update_node_info(entry)),
                    Err(_) => Err(StorageIOError::read_logs(to_any_error(current_err))),
                }
            }
        }
    }

    /// [`Self::deserialize_entry`] with the entry index threaded into the
    /// error so the operator-facing fatal names what is damaged and how to
    /// recover (bug-raft-log-bincode-format-instability item 4).
    fn deserialize_entry_at(
        key: &[u8],
        bytes: &[u8],
    ) -> Result<Entry<FerrosRaftConfig>, StorageIOError<u64>> {
        Self::deserialize_entry(bytes).map_err(|e| {
            StorageIOError::read_logs(to_any_error(UnreadableLogEntry {
                index: Self::index_from_key(key),
                detail: e.to_string(),
            }))
        })
    }

    /// The `LogId` of the entry stored at `index`, if it is still present.
    ///
    /// A clamped purge has to name a real `LogId`, not just an index -- the
    /// term at the clamp point is not necessarily the term of the purge
    /// request that was refused.
    fn log_id_at(&self, index: u64) -> Result<Option<LogId<u64>>, StorageIOError<u64>> {
        let key = Self::index_key(index);
        match self
            .log
            .get(key)
            .map_err(|e| StorageIOError::read_logs(to_any_error(e)))?
        {
            Some(bytes) => Ok(Some(
                Self::deserialize_entry_at(&Self::index_key(index), &bytes)?.log_id,
            )),
            None => Ok(None),
        }
    }

    fn save_meta<T: serde::Serialize>(
        meta: &sled::Tree,
        key: &[u8],
        value: &T,
    ) -> Result<(), StorageIOError<u64>> {
        let bytes =
            bincode::serialize(value).map_err(|e| StorageIOError::write(to_any_error(e)))?;
        meta.insert(key, bytes)
            .map_err(|e| StorageIOError::write(to_any_error(e)))?;
        Ok(())
    }

    fn load_meta<T: serde::de::DeserializeOwned>(
        meta: &sled::Tree,
        key: &[u8],
    ) -> Result<Option<T>, StorageIOError<u64>> {
        match meta
            .get(key)
            .map_err(|e| StorageIOError::read(to_any_error(e)))?
        {
            Some(bytes) => {
                let val = bincode::deserialize(&bytes)
                    .map_err(|e| StorageIOError::read(to_any_error(e)))?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Return the persisted `last_purged_log_id`, if any.
    ///
    /// Used during startup recovery: if the state machine's `last_applied` is
    /// `None` (e.g., after an OOM kill lost the in-memory state), the purge
    /// point serves as a safe baseline — entries can only be purged after
    /// they've been applied and snapshotted.
    pub fn last_purged_log_id(&self) -> Result<Option<LogId<u64>>, StorageIOError<u64>> {
        Self::load_meta(&self.meta, META_LAST_PURGED)
    }

    /// Scan all log entries for the last `Membership` payload.
    ///
    /// Used during recovery when the state machine lost `last_membership`
    /// (e.g., after OOM kill). Walks the entire log backwards and returns
    /// the most recent membership entry, if any.
    pub fn find_last_membership(
        &self,
    ) -> Result<Option<openraft::StoredMembership<u64, openraft::BasicNode>>, StorageIOError<u64>>
    {
        use openraft::EntryPayload;
        // Iterate backwards (last entry first) through the log.
        for item in self.log.iter().rev() {
            let (k, v) = item.map_err(|e| StorageIOError::read_logs(to_any_error(e)))?;
            let entry = Self::deserialize_entry_at(&k, &v)?;
            if let EntryPayload::Membership(membership) = entry.payload {
                return Ok(Some(openraft::StoredMembership::new(
                    Some(entry.log_id),
                    membership,
                )));
            }
        }
        Ok(None)
    }

    /// Reconstruct committed topology state from normal Raft log entries.
    ///
    /// This is the restart fallback when the state-machine snapshot is absent
    /// or stale: we replay only topology-affecting commands from oldest to
    /// newest and rebuild the committed member/token view.
    pub fn recover_topology_state(&self) -> Result<RecoveredTopology, StorageIOError<u64>> {
        use openraft::EntryPayload;

        let mut members = BTreeMap::new();
        let mut token_map = BTreeMap::new();

        for item in self.log.iter() {
            let (k, v) = item.map_err(|e| StorageIOError::read_logs(to_any_error(e)))?;
            let entry = Self::deserialize_entry_at(&k, &v)?;
            let EntryPayload::Normal(cmd) = entry.payload else {
                continue;
            };

            match cmd.op {
                RaftOp::JoinNode(node) | RaftOp::UpdateNodeInfo(node) => {
                    members.insert(super::uuid_to_node_id(node.host_id), node);
                }
                RaftOp::LeaveNode { node_id } => {
                    members.remove(&node_id);
                    token_map.retain(|_, owner| *owner != node_id);
                }
                RaftOp::AssignTokens { node_id, tokens } => {
                    token_map.retain(|_, owner| *owner != node_id);
                    for token in tokens {
                        token_map.insert(token, node_id);
                    }
                }
                RaftOp::SetNodeState { node_id, state } => {
                    if let Some(node) = members.get_mut(&node_id) {
                        node.state = state;
                    }
                }
                _ => {}
            }
        }

        Ok(RecoveredTopology { members, token_map })
    }

    /// Return the last entry currently present in the log tree.
    fn last_entry_log_id(&self) -> Result<Option<LogId<u64>>, StorageIOError<u64>> {
        let last = self
            .log
            .last()
            .map_err(|e| StorageIOError::read_logs(to_any_error(e)))?;
        match last {
            Some((k, v)) => {
                let entry = Self::deserialize_entry_at(&k, &v)?;
                Ok(Some(entry.log_id))
            }
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader
// ---------------------------------------------------------------------------

impl RaftLogReader<FerrosRaftConfig> for SledLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<FerrosRaftConfig>>, StorageError<u64>> {
        let start_bytes = match range.start_bound() {
            std::ops::Bound::Included(&idx) => std::ops::Bound::Included(Self::index_key(idx)),
            std::ops::Bound::Excluded(&idx) => std::ops::Bound::Excluded(Self::index_key(idx)),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        let end_bytes = match range.end_bound() {
            std::ops::Bound::Included(&idx) => std::ops::Bound::Included(Self::index_key(idx)),
            std::ops::Bound::Excluded(&idx) => std::ops::Bound::Excluded(Self::index_key(idx)),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };

        let mut entries = Vec::new();
        for item in self.log.range((start_bytes, end_bytes)) {
            let (k, v) = item.map_err(|e| StorageIOError::read_logs(to_any_error(e)))?;
            let entry = Self::deserialize_entry_at(&k, &v)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// RaftLogStorage
// ---------------------------------------------------------------------------

impl openraft::storage::RaftLogStorage<FerrosRaftConfig> for SledLogStore {
    type LogReader = SledLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<FerrosRaftConfig>, StorageError<u64>> {
        let last_purged: Option<LogId<u64>> = Self::load_meta(&self.meta, META_LAST_PURGED)?;

        let last_in_log = self.last_entry_log_id()?;

        let last_log_id = last_in_log.or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // sled trees are cheaply cloneable (they share the inner Arc).
        SledLogStore {
            db: self.db.clone(),
            log: self.log.clone(),
            meta: self.meta.clone(),
            // Same cell, not a copy: a reader that purges must see the same
            // durable watermark the writer does.
            durable_applied: self.durable_applied.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let meta = self.meta.clone();
        let bytes =
            bincode::serialize(vote).map_err(|e| StorageIOError::write_vote(to_any_error(e)))?;
        ferrosa_net::task_pool::TaskPool::current("raft-log-store")
            .spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
                meta.insert(META_VOTE, bytes)
                    .map_err(|e| Box::new(StorageIOError::write_vote(to_any_error(e))))?;
                meta.flush()
                    .map_err(|e| Box::new(StorageIOError::write_vote(to_any_error(e))))?;
                Ok(())
            })
            .await
            .map_err(|e| StorageIOError::write_vote(to_any_error(e)))?
            .map_err(|e| *e)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(Self::load_meta(&self.meta, META_VOTE)?)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        // W1.19b: persist + fsync so an OS crash between save_committed and
        // the next vote does not lose the committed marker. sled flushes
        // its WAL on drop, but in a hard-crash scenario the in-memory
        // index would be lost; an explicit flush turns this insert into a
        // durability barrier.
        Self::save_meta(&self.meta, META_COMMITTED, &committed)?;
        let meta = self.meta.clone();
        ferrosa_net::task_pool::TaskPool::current("raft-log-store")
            .spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
                meta.flush()
                    .map_err(|e| Box::new(StorageIOError::write(to_any_error(e))))?;
                Ok(())
            })
            .await
            .map_err(|e| StorageIOError::write(to_any_error(e)))?
            .map_err(|e| *e)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(Self::load_meta::<Option<LogId<u64>>>(&self.meta, META_COMMITTED)?.unwrap_or(None))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<FerrosRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<FerrosRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        // W1.19a: any error path in `append` must still fire the LogFlushed
        // callback (with Err) so openraft's Raft loop sees the failure
        // synchronously instead of waiting on a oneshot that never sends.
        // Build the batch first; serialization errors are surfaced by the
        // helper below so they reach the callback too.
        let result = self.append_inner(entries).await;

        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => callback.log_io_completed(Err(std::io::Error::other(format!(
                "log append failed: {e}"
            )))),
        }

        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let log = self.log.clone();
        let start = Self::index_key(log_id.index);
        ferrosa_net::task_pool::TaskPool::current("raft-log-store")
            .spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
                let keys_to_remove: Vec<sled::IVec> = log
                    .range(start..)
                    .filter_map(|r| r.ok().map(|(k, _v)| k))
                    .collect();
                let mut batch = sled::Batch::default();
                for key in keys_to_remove {
                    batch.remove(key);
                }
                log.apply_batch(batch)
                    .map_err(|e| Box::new(StorageIOError::write_logs(to_any_error(e))))?;
                Ok(())
            })
            .await
            .map_err(|e| StorageIOError::write_logs(to_any_error(e)))?
            .map_err(|e| *e)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Never delete past what the state machine has durably applied.
        //
        // openraft only asks to purge up to a snapshot it believes exists, so
        // normally this passes the request through untouched. It exists for
        // when that belief and the disk disagree: the snapshot is a separate
        // file with its own durability, so a crash between "snapshot written"
        // and "snapshot durable" leaves this store free to delete entries that
        // survive nowhere else on this node. That is how node3 was stranded on
        // 2026-08-20 -- purged to 3065, applied to 2905, and the entries in
        // between gone for good.
        //
        // Purging less than asked costs disk until the next purge. Purging
        // more than is durable cannot be undone.
        let durable = self
            .durable_applied
            .load(std::sync::atomic::Ordering::Acquire);
        let log_id = match crate::raft::local_state::purge_ceiling(log_id.index, Some(durable)) {
            crate::raft::local_state::PurgeDecision::Purge { .. } => log_id,
            crate::raft::local_state::PurgeDecision::Clamp { through, requested } => {
                tracing::warn!(
                    requested,
                    clamped_to = through,
                    "refusing to purge Raft log past the durably applied index; \
                     the entries above it exist nowhere else on this node. \
                     Purging only what the persisted snapshot covers."
                );
                match self.log_id_at(through) {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        tracing::warn!(
                            through,
                            "clamped purge point is not present in the log; \
                             skipping this purge"
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(
                            %e,
                            through,
                            "could not read the clamped purge point; skipping this purge"
                        );
                        return Ok(());
                    }
                }
            }
            crate::raft::local_state::PurgeDecision::Skip { requested } => {
                tracing::warn!(
                    requested,
                    "refusing to purge Raft log: no snapshot is known to be durable, \
                     so no entry is known to be reconstructable"
                );
                return Ok(());
            }
        };

        // Persist the purge point first so recovery can reconstruct state.
        Self::save_meta(&self.meta, META_LAST_PURGED, &log_id)?;

        // Remove all entries with index <= log_id.index.
        // Big-endian keys: 0x0000..0000 through index_key(log_id.index) inclusive.
        // Run sled disk IO on a blocking thread so the async Raft runtime
        // stays responsive for heartbeat processing — under sustained
        // AppendEntries traffic, a synchronous purge of a large segment
        // would stall the lane and miss heartbeat deadlines (W1.20 / I-25).
        let log = self.log.clone();
        let end_inclusive = Self::index_key(log_id.index);
        ferrosa_net::task_pool::TaskPool::current("raft-log-store")
            .spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
                let keys_to_remove: Vec<sled::IVec> = log
                    .range(..=end_inclusive)
                    .filter_map(|r| r.ok().map(|(k, _v)| k))
                    .collect();

                let mut batch = sled::Batch::default();
                for key in keys_to_remove {
                    batch.remove(key);
                }
                log.apply_batch(batch)
                    .map_err(|e| Box::new(StorageIOError::write_logs(to_any_error(e))))?;
                Ok(())
            })
            .await
            .map_err(|e| StorageIOError::write_logs(to_any_error(e)))?
            .map_err(|e| *e)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Convert an error into an `AnyError` for openraft storage errors.
fn to_any_error(e: impl std::error::Error + Send + Sync + 'static) -> AnyError {
    AnyError::new(&e)
}

/// Lowercase hex of the first `max` bytes of `bytes`.
fn hex_preview(bytes: &[u8], max: usize) -> String {
    bytes.iter().take(max).map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, EntryPayload};

    /// Helper: create a blank entry at the given index.
    fn blank_entry(term: u64, index: u64) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    fn normal_entry(term: u64, index: u64, op: RaftOp) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(RaftCommand {
                op,
                schema_version: Uuid::new_v4(),
            }),
        }
    }

    // -- append_and_read_back ---------------------------------------------

    #[tokio::test]
    async fn append_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        let entries = vec![blank_entry(1, 1), blank_entry(1, 2), blank_entry(1, 3)];

        // We can't construct a real LogFlushed (its constructor is pub(crate)
        // inside openraft), so we use the RaftLogReader trait directly and
        // insert via the sled batch path that `append` uses internally.
        {
            let mut batch = sled::Batch::default();
            for entry in &entries {
                let key = SledLogStore::index_key(entry.log_id.index);
                let val = SledLogStore::serialize_entry(entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        let read_back: Vec<Entry<FerrosRaftConfig>> =
            store.try_get_log_entries(1u64..4u64).await.unwrap();

        assert_eq!(read_back.len(), 3);
        for (i, entry) in read_back.iter().enumerate() {
            assert_eq!(entry.log_id.index, (i as u64) + 1);
        }
    }

    // -- purge_removes_old_entries ----------------------------------------

    /// The guard has to hold through the real `purge`, not only in the pure
    /// decision function -- an inert guard reads exactly like a working one.
    ///
    /// Entries 1..=5 exist and the snapshot covers 3. openraft asks to purge
    /// through 5. Deleting 4 and 5 would remove this node's only copy of them,
    /// which is what stranded node3 on 2026-08-20, so the purge must stop at 3.
    #[tokio::test]
    async fn purge_stops_at_the_durably_applied_index() {
        use openraft::storage::RaftLogStorage;

        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                batch.insert(
                    &SledLogStore::index_key(idx),
                    SledLogStore::serialize_entry(&entry).unwrap(),
                );
            }
            store.log.apply_batch(batch).unwrap();
        }

        // The snapshot on disk only covers index 3.
        store
            .durable_applied
            .store(3, std::sync::atomic::Ordering::Release);

        store
            .purge(LogId::new(CommittedLeaderId::new(1, 0), 5))
            .await
            .unwrap();

        let remaining = store.try_get_log_entries(1u64..6u64).await.unwrap();
        let indexes: Vec<u64> = remaining.iter().map(|e| e.log_id.index).collect();
        assert_eq!(
            indexes,
            vec![4, 5],
            "entries above the durable snapshot must survive the purge"
        );

        let state = <SledLogStore as RaftLogStorage<FerrosRaftConfig>>::get_log_state(&mut store)
            .await
            .unwrap();
        assert_eq!(
            state.last_purged_log_id.map(|id| id.index),
            Some(3),
            "the recorded purge point must match what was actually deleted, or \
             the node restarts believing entries are gone that are not"
        );

        assert_eq!(
            crate::raft::local_state::classify_local_raft_state(Some(3), Some(3)),
            crate::raft::local_state::LocalRaftState::Usable
        );
    }

    #[tokio::test]
    async fn purge_removes_old_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=5 directly.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // A purge may not run past what the state machine has durably applied,
        // so say that a snapshot covering these entries exists. Without this
        // the store correctly refuses to delete anything -- deleting entries
        // no snapshot covers is what strands a node.
        store
            .durable_applied
            .store(5, std::sync::atomic::Ordering::Release);

        // Purge up to index 3 (inclusive).
        let purge_id = LogId::new(CommittedLeaderId::new(1, 0), 3);
        store.purge(purge_id).await.unwrap();

        // Entries 1-3 should be gone.
        let remaining = store.try_get_log_entries(1u64..6u64).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 4);
        assert_eq!(remaining[1].log_id.index, 5);

        // last_purged should be recorded.
        let state =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_state(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(state.last_purged_log_id, Some(purge_id));
    }

    // -- vote_persistence -------------------------------------------------

    #[tokio::test]
    async fn vote_persistence() {
        let dir = tempfile::tempdir().unwrap();

        let vote = Vote::new(5, 42);

        // Save via first store instance.
        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_vote(
                &mut store, &vote,
            )
            .await
            .unwrap();
        }

        // Re-open and read back.
        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            let read_back =
                <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_vote(
                    &mut store,
                )
                .await
                .unwrap();
            assert_eq!(read_back, Some(vote));
        }
    }

    // -- empty_store_state ------------------------------------------------

    #[tokio::test]
    async fn empty_store_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        let state =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_state(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, None);

        let vote =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_vote(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(vote, None);
    }

    // -- get_log_state with entries present --------------------------------

    #[tokio::test]
    async fn get_log_state_returns_last_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=3 directly.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=3 {
                let entry = blank_entry(2, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        let state =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_state(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert!(state.last_log_id.is_some());
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    /// W1.19c — round-trip an entry through the new
    /// magic-prefix framing.  The bytes must start with `FRE1` + the
    /// version tag, and decode back into the same entry.
    #[test]
    fn entry_with_magic_prefix_roundtrips() {
        let entry = normal_entry(
            3,
            7,
            RaftOp::LeaveNode {
                node_id: 0xDEAD_BEEFu64,
            },
        );
        let bytes = SledLogStore::serialize_entry(&entry).unwrap();
        assert_eq!(
            &bytes[..4],
            &SledLogStore::ENTRY_MAGIC,
            "tagged entry must start with the magic prefix",
        );
        assert_eq!(
            bytes[4],
            SledLogStore::ENTRY_FORMAT_VERSION,
            "fifth byte is the format version tag",
        );
        let decoded = SledLogStore::deserialize_entry(&bytes).unwrap();
        match decoded.payload {
            EntryPayload::Normal(RaftCommand {
                op: RaftOp::LeaveNode { node_id },
                ..
            }) => assert_eq!(node_id, 0xDEAD_BEEFu64),
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    /// W1.19c — legacy entries (no magic prefix) still decode.
    /// Asserts the back-compat fallthrough remains in place.
    #[test]
    fn legacy_log_decode_unambiguous() {
        // Hand-craft a bare bincoded Entry — legacy on-disk form.
        let entry = normal_entry(
            5,
            11,
            RaftOp::AssignTokens {
                node_id: 42,
                tokens: vec![100, 200, 300],
            },
        );
        let bare = bincode::serialize(&entry).unwrap();
        // Untagged decode goes through the legacy fallthrough.
        let decoded = SledLogStore::deserialize_entry(&bare).unwrap();
        match decoded.payload {
            EntryPayload::Normal(RaftCommand {
                op: RaftOp::AssignTokens { node_id, tokens },
                ..
            }) => {
                assert_eq!(node_id, 42);
                assert_eq!(tokens, vec![100, 200, 300]);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    /// W1.19c — an unknown future format version is rejected loudly
    /// instead of being silently reinterpreted as legacy data.
    #[test]
    fn entry_with_unknown_version_fails_loud() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SledLogStore::ENTRY_MAGIC);
        bytes.push(0xFF); // version we do not understand
        bytes.extend_from_slice(&[0u8; 64]);
        let err = SledLogStore::deserialize_entry(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported entry format version"),
            "expected unknown-version error, got: {msg}",
        );
    }

    #[test]
    fn deserialize_entry_reads_pre_update_node_info_log_format() {
        let schema_version = Uuid::new_v4();
        let legacy_entry = openraft::Entry::<LegacyFerrosRaftConfigPreUpdateNodeInfo> {
            log_id: LogId::new(CommittedLeaderId::new(7, 3), 42),
            payload: EntryPayload::Normal(LegacyRaftCommandPreUpdateNodeInfo {
                op: LegacyRaftOpPreUpdateNodeInfo::LeaveNode { node_id: 9 },
                schema_version,
            }),
        };

        let encoded = bincode::serialize(&legacy_entry).unwrap();
        let decoded = SledLogStore::deserialize_entry(&encoded).unwrap();

        match decoded.payload {
            EntryPayload::Normal(RaftCommand {
                op: RaftOp::LeaveNode { node_id },
                schema_version: decoded_version,
            }) => {
                assert_eq!(node_id, 9);
                assert_eq!(decoded_version, schema_version);
            }
            other => panic!("expected legacy LeaveNode to decode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn recover_topology_state_replays_topology_commands_from_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = SledLogStore::new(dir.path()).unwrap();
        let node1 = Uuid::from_u128(1);
        let node2 = Uuid::from_u128(2);
        let node1_id = super::super::uuid_to_node_id(node1);
        let node2_id = super::super::uuid_to_node_id(node2);

        let mut batch = sled::Batch::default();
        for entry in [
            normal_entry(
                1,
                1,
                RaftOp::JoinNode(NodeInfo {
                    host_id: node1,
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Joining,
                    cql_broadcast: Some("127.0.0.1:19042".to_string()),
                }),
            ),
            normal_entry(
                1,
                2,
                RaftOp::JoinNode(NodeInfo {
                    host_id: node2,
                    addr: "10.0.0.2:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Joining,
                    cql_broadcast: Some("127.0.0.1:19043".to_string()),
                }),
            ),
            normal_entry(
                1,
                3,
                RaftOp::AssignTokens {
                    node_id: node1_id,
                    tokens: vec![-10, 10],
                },
            ),
            normal_entry(
                1,
                4,
                RaftOp::AssignTokens {
                    node_id: node2_id,
                    tokens: vec![20, 30],
                },
            ),
            normal_entry(
                1,
                5,
                RaftOp::SetNodeState {
                    node_id: node1_id,
                    state: NodeState::Normal,
                },
            ),
            normal_entry(
                1,
                6,
                RaftOp::UpdateNodeInfo(NodeInfo {
                    host_id: node2,
                    addr: "10.0.0.22:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack2".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: Some("127.0.0.1:29043".to_string()),
                }),
            ),
            normal_entry(1, 7, RaftOp::LeaveNode { node_id: node1_id }),
        ] {
            let key = SledLogStore::index_key(entry.log_id.index);
            let val = SledLogStore::serialize_entry(&entry).unwrap();
            batch.insert(&key, val);
        }
        store.log.apply_batch(batch).unwrap();

        let topology = store.recover_topology_state().unwrap();

        assert_eq!(topology.members.len(), 1);
        let node2_info = topology.members.get(&node2_id).unwrap();
        assert_eq!(node2_info.addr, "10.0.0.22:7000");
        assert_eq!(node2_info.rack, "rack2");
        assert_eq!(node2_info.cql_broadcast.as_deref(), Some("127.0.0.1:29043"));
        assert_eq!(node2_info.state, NodeState::Normal);
        assert_eq!(topology.token_map.len(), 2);
        assert_eq!(topology.token_map.get(&20), Some(&node2_id));
        assert_eq!(topology.token_map.get(&30), Some(&node2_id));
        assert!(!topology.token_map.values().any(|owner| *owner == node1_id));
    }

    // -- save_committed / read_committed round-trip -----------------------

    #[tokio::test]
    async fn committed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Initially no committed value.
        let initial =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_committed(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(initial, None);

        // Save a committed log id.
        let committed = Some(LogId::new(CommittedLeaderId::new(3, 0), 7));
        <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_committed(
            &mut store, committed,
        )
        .await
        .unwrap();

        // Read back.
        let read_back =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_committed(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(read_back, committed);
    }

    // -- truncate removes entries from given index onward ------------------

    #[tokio::test]
    async fn truncate_removes_tail_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=5.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Truncate from index 3 onward (3, 4, 5 removed).
        let truncate_id = LogId::new(CommittedLeaderId::new(1, 0), 3);
        <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::truncate(
            &mut store,
            truncate_id,
        )
        .await
        .unwrap();

        // Only entries 1 and 2 should remain.
        let remaining = store.try_get_log_entries(1u64..6u64).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
    }

    // -- get_log_reader returns independent reader -------------------------

    #[tokio::test]
    async fn get_log_reader_returns_reader_with_same_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert an entry
        {
            let mut batch = sled::Batch::default();
            let entry = blank_entry(1, 1);
            let key = SledLogStore::index_key(1);
            let val = SledLogStore::serialize_entry(&entry).unwrap();
            batch.insert(&key, val);
            store.log.apply_batch(batch).unwrap();
        }

        let mut reader =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_reader(
                &mut store,
            )
            .await;

        let entries = reader.try_get_log_entries(1u64..2u64).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    // -- range queries with various bounds --------------------------------

    #[tokio::test]
    async fn try_get_log_entries_empty_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=3
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=3 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Range that doesn't match any entries
        let entries = store.try_get_log_entries(10u64..20u64).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn try_get_log_entries_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Get exactly one entry
        let entries = store.try_get_log_entries(3u64..4u64).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 3);
    }

    // -- reset ------------------------------------------------------------

    #[tokio::test]
    async fn reset_replaces_log_and_meta_with_empty_store() {
        let dir = tempfile::tempdir().unwrap();

        // Pre-populate: 3 log entries + a vote + a committed marker.
        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            let mut batch = sled::Batch::default();
            for idx in 1u64..=3 {
                let entry = blank_entry(7, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();

            let vote = Vote::new(7, 99);
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_vote(
                &mut store, &vote,
            )
            .await
            .unwrap();

            let committed = Some(LogId::new(CommittedLeaderId::new(7, 0), 3));
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_committed(
                &mut store, committed,
            )
            .await
            .unwrap();

            store.db.flush().unwrap();
        } // drop releases sled lock

        let counts = SledLogStore::reset(dir.path()).expect("reset must succeed on a free dir");
        assert_eq!(counts.log_entries, None);
        assert_eq!(counts.meta_keys, None);
        assert!(
            counts.backup_path.as_ref().is_some_and(|p| p.exists()),
            "reset must retain the previous raft dir as a backup"
        );

        // Reopen and confirm trees are empty.
        let mut store = SledLogStore::new(dir.path()).unwrap();
        let remaining = store.try_get_log_entries(0u64..100u64).await.unwrap();
        assert!(remaining.is_empty(), "log tree must be empty after reset");
        let vote_after =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_vote(
                &mut store,
            )
            .await
            .unwrap();
        assert!(vote_after.is_none(), "vote must be cleared after reset");
    }

    #[test]
    fn reset_replaces_raft_dir_and_leaves_fresh_dir_immediately_openable() {
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().to_path_buf();
        let marker = original_path.join("operator-note.txt");
        std::fs::write(&marker, b"kept with reset backup").unwrap();

        {
            let db = SledLogStore::open_sled_db_with_lock_retry(&original_path).unwrap();
            let log = db.open_tree("log").unwrap();
            log.insert(1u64.to_be_bytes(), b"entry".to_vec()).unwrap();
            db.flush().unwrap();
        }

        let summary = SledLogStore::reset(&original_path).expect("reset must replace raft dir");
        let backup = summary
            .backup_path
            .expect("reset must report the retained backup path");

        assert!(original_path.exists(), "reset must recreate the raft dir");
        assert!(backup.exists(), "previous raft dir must be retained");
        assert!(
            backup.join("operator-note.txt").exists(),
            "backup must contain the previous directory contents"
        );

        let fresh = SledLogStore::new(&original_path)
            .expect("fresh raft dir must be immediately openable without sled lock race");
        assert_eq!(fresh.log.len(), 0, "fresh log tree must be empty");
        assert_eq!(fresh.meta.len(), 0, "fresh meta tree must be empty");
    }

    #[test]
    fn reset_on_empty_store_replaces_dir_and_reports_backup() {
        let dir = tempfile::tempdir().unwrap();
        // Touch a store so the on-disk layout exists, then drop.
        let _ = SledLogStore::new(dir.path()).unwrap();

        let counts = SledLogStore::reset(dir.path()).expect("reset on empty store must succeed");
        assert_eq!(counts.log_entries, None);
        assert_eq!(counts.meta_keys, None);
        assert!(counts.backup_path.as_ref().is_some_and(|p| p.exists()));
    }

    /// W1.19b: save_committed must flush the meta tree so the committed
    /// marker survives an OS crash between save and the next vote. We
    /// simulate the durability barrier by saving the marker, then dropping
    /// and reopening the store, and asserting the marker round-trips. (A
    /// stronger crash-injection test would require process kill; this is
    /// the strongest test feasible without that infra.)
    #[tokio::test]
    async fn save_committed_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let committed = Some(LogId::new(CommittedLeaderId::new(11, 0), 42));

        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_committed(
                &mut store, committed,
            )
            .await
            .unwrap();
            // Drop without an extra db.flush() — save_committed must have
            // flushed the meta tree itself.
        }

        // Reopen and read back.
        let mut store = SledLogStore::new(dir.path()).unwrap();
        let read_back =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_committed(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(
            read_back, committed,
            "save_committed must persist + flush before returning"
        );
    }

    /// W1.20: `purge` must offload sled writes to a blocking thread so the
    /// async runtime keeps making progress. Acceptance: a current_thread
    /// runtime executes a 1000-entry purge concurrently with a sibling
    /// task, and the sibling task's heartbeats keep ticking. If `purge`
    /// did its sled work synchronously on the runtime thread, the
    /// heartbeats would stall for the duration of the IO.
    #[tokio::test(flavor = "current_thread")]
    async fn purge_does_not_block_heartbeats() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Pre-populate 1000 entries.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=1000 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
            store.log.flush().unwrap();
        }

        // Sibling heartbeat task: increments a counter every yield.
        let ticks = Arc::new(AtomicU64::new(0));
        let ticks_clone = ticks.clone();
        let stop = Arc::new(AtomicU64::new(0));
        let stop_clone = stop.clone();
        let heartbeat = tokio::spawn(async move {
            while stop_clone.load(Ordering::Relaxed) == 0 {
                ticks_clone.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });

        // Run the purge. With spawn_blocking inside purge, the
        // current_thread runtime is free to keep ticking the heartbeat
        // task while the sled work runs on a blocking thread.
        // Declare a durable snapshot covering the range, or the purge guard
        // refuses and this stops measuring what it means to measure.
        store
            .durable_applied
            .store(1000, std::sync::atomic::Ordering::Release);

        let purge_id = LogId::new(CommittedLeaderId::new(1, 0), 1000);
        store.purge(purge_id).await.unwrap();

        let mid_ticks = ticks.load(Ordering::Relaxed);
        // Stop the heartbeat and wait for it to finish.
        stop.store(1, Ordering::Relaxed);
        let _ = heartbeat.await;

        // The heartbeat must have made progress while purge was in
        // flight. The exact lower bound is conservative — in CI with
        // contention, even one tick proves the runtime kept advancing.
        assert!(
            mid_ticks > 0,
            "the sibling task must accumulate ticks while purge is running; \
             got {mid_ticks} — purge appears to have blocked the runtime"
        );

        // Verify purge actually purged.
        let remaining = store.try_get_log_entries(1u64..1001u64).await.unwrap();
        assert!(remaining.is_empty(), "all 1000 entries should be purged");
    }

    #[test]
    fn reset_missing_dir_creates_fresh_dir_without_backup() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("raft");

        let counts = SledLogStore::reset(&missing).expect("reset must create missing raft dir");

        assert!(missing.is_dir());
        assert_eq!(counts.log_entries, None);
        assert_eq!(counts.meta_keys, None);
        assert_eq!(counts.backup_path, None);
    }

    // -- offline log inspect / truncate (raft-log operator tooling) --------

    /// Bytes that fail every decode path (tagged, current bare, legacy
    /// pre-UpdateNodeInfo). ASCII text reproduces the real-world failure
    /// shape from bug-raft-log-bincode-format-instability: a value blob
    /// misread as an enum tag.
    fn undecodable_bytes() -> Vec<u8> {
        b"avioactive-not-a-raft-entry-written-by-a-drifted-build".to_vec()
    }

    /// Populate a store at `path` with blank entries for `indexes`, then
    /// overwrite `bad` indexes with undecodable bytes. Drops the store so
    /// the sled lock is released for the offline functions.
    async fn seed_store(path: &Path, indexes: std::ops::RangeInclusive<u64>, bad: &[u64]) {
        let mut store = SledLogStore::new(path).unwrap();
        let mut batch = sled::Batch::default();
        for idx in indexes {
            let entry = blank_entry(2, idx);
            batch.insert(
                &SledLogStore::index_key(idx),
                SledLogStore::serialize_entry(&entry).unwrap(),
            );
        }
        for &idx in bad {
            batch.insert(&SledLogStore::index_key(idx), undecodable_bytes());
        }
        store.log.apply_batch(batch).unwrap();

        let committed = Some(LogId::new(CommittedLeaderId::new(2, 0), 5));
        <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_committed(
            &mut store, committed,
        )
        .await
        .unwrap();
        let vote = Vote::new(2, 1);
        <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_vote(
            &mut store, &vote,
        )
        .await
        .unwrap();
        store.db.flush().unwrap();
    }

    #[tokio::test]
    async fn inspect_reports_healthy_log() {
        let dir = tempfile::tempdir().unwrap();
        seed_store(dir.path(), 1..=5, &[]).await;

        let report = SledLogStore::inspect(dir.path()).expect("inspect healthy store");

        assert_eq!(report.total_entries, 5);
        assert_eq!(report.decoded_entries, 5);
        assert_eq!(report.undecodable_count, 0);
        assert_eq!(report.first_index, Some(1));
        assert_eq!(report.last_index, Some(5));
        assert!(report.first_undecodable.is_none());
        assert_eq!(report.committed.map(|c| c.index), Some(5));
        assert_eq!(report.vote, Some(Vote::new(2, 1)));
        assert_eq!(report.last_purged, None);
    }

    #[tokio::test]
    async fn inspect_locates_first_undecodable_entry() {
        let dir = tempfile::tempdir().unwrap();
        // Good 1..=3 and 5; index 4 is drifted-build garbage.
        seed_store(dir.path(), 1..=5, &[4]).await;

        let report = SledLogStore::inspect(dir.path()).expect("inspect damaged store");

        assert_eq!(report.total_entries, 5);
        assert_eq!(report.decoded_entries, 4);
        assert_eq!(report.undecodable_count, 1);
        let bad = report
            .first_undecodable
            .expect("must locate the first undecodable entry");
        assert_eq!(bad.index, 4);
        assert!(!bad.error.is_empty());
        assert!(
            bad.preview_hex.starts_with("6176696f"),
            "preview must show the raw on-disk bytes, got {}",
            bad.preview_hex
        );
    }

    #[test]
    fn is_lock_contention_classifies_only_the_lock_error() {
        let lock = sled::Error::Io(std::io::Error::other(
            "could not acquire lock on \"/tmp/x/db\": WouldBlock",
        ));
        assert!(SledLogStore::is_lock_contention(&lock));
        let wouldblock = sled::Error::Io(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "resource temporarily unavailable",
        ));
        assert!(SledLogStore::is_lock_contention(&wouldblock));
        // A genuine corruption fault must NOT be retried.
        let corrupt = sled::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "torn page",
        ));
        assert!(!SledLogStore::is_lock_contention(&corrupt));
    }

    /// Regression for the offline-open lock race (CI flake in
    /// `inspect_locates_first_undecodable_entry`): a still-releasing handle on
    /// the same path must not fail an offline open. Hold the store, start the
    /// offline open concurrently, release shortly after — `open_offline` must
    /// retry through the transient lock and succeed.
    #[tokio::test]
    async fn open_offline_retries_through_a_transient_lock() {
        let dir = tempfile::tempdir().unwrap();
        seed_store(dir.path(), 1..=3, &[]).await;

        let holder = SledLogStore::new(dir.path()).expect("hold the directory lock");
        let path = dir.path().to_path_buf();
        let releaser = std::thread::spawn(move || {
            // Release well within the retry budget (10 × 50 ms ≈ 500 ms).
            std::thread::sleep(Duration::from_millis(120));
            drop(holder);
        });

        // (`new`/`open_offline` now share the same transient-lock retry, so a
        // plain open no longer fails fast on a still-releasing holder — that
        // exact behaviour is asserted by `new_retries_through_a_transient_lock`.)
        let store = SledLogStore::open_offline(&path)
            .expect("open_offline must retry past the transient lock and succeed");
        drop(store);
        releaser.join().unwrap();
    }

    /// `SledLogStore::new` must retry through a *transient* directory-lock
    /// contention (sled's `EWOULDBLOCK`), not fail hard. The online open is
    /// used on a fresh/just-released data dir where another handle may still be
    /// releasing sled's lock, or where heavy parallel I/O makes the `flock`
    /// momentarily return `Resource temporarily unavailable`. A live peer holds
    /// the lock for its whole lifetime (≫ the ~500 ms retry budget), so a real
    /// dual-open conflict still surfaces an error — only the millisecond-scale
    /// transient is absorbed. (Fixes the CI `WouldBlock` panics on a fresh
    /// tempdir; see ../specs/bug-idle-cpu-spin-3cores.md follow-ups.)
    #[tokio::test]
    async fn new_retries_through_a_transient_lock() {
        let dir = tempfile::tempdir().unwrap();
        seed_store(dir.path(), 1..=3, &[]).await;

        let holder = SledLogStore::new(dir.path()).expect("hold the directory lock");
        let path = dir.path().to_path_buf();
        let releaser = std::thread::spawn(move || {
            // Release well within the retry budget (10 × 50 ms ≈ 500 ms).
            std::thread::sleep(Duration::from_millis(120));
            drop(holder);
        });

        // The online open must retry past the transient lock and succeed —
        // exactly like `open_offline`.
        let store =
            SledLogStore::new(&path).expect("new() must retry past the transient lock and succeed");
        drop(store);
        releaser.join().unwrap();
    }

    #[tokio::test]
    async fn truncate_from_removes_tail_and_clamps_committed() {
        let dir = tempfile::tempdir().unwrap();
        // committed=5 (set by seed_store); truncate from 4.
        seed_store(dir.path(), 1..=5, &[4]).await;

        let report = SledLogStore::truncate_from(dir.path(), 4).expect("truncate damaged tail");

        assert_eq!(report.removed_entries, 2);
        assert_eq!(report.first_removed_index, Some(4));
        assert_eq!(report.new_last_index, Some(3));
        assert_eq!(report.committed_before.map(|c| c.index), Some(5));
        assert_eq!(
            report.committed_after.map(|c| c.index),
            Some(3),
            "committed marker must be clamped to the new last entry"
        );

        // Re-open and verify on-disk state matches the report.
        let mut store = SledLogStore::new(dir.path()).unwrap();
        let remaining = store.try_get_log_entries(1u64..10u64).await.unwrap();
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining.last().unwrap().log_id.index, 3);
        let committed =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_committed(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(committed.map(|c| c.index), Some(3));
    }

    #[tokio::test]
    async fn truncate_from_to_empty_log_clamps_committed_to_last_purged() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            // Snapshot covered through 2; live entries 3..=4.
            let purged: LogId<u64> = LogId::new(CommittedLeaderId::new(1, 0), 2);
            SledLogStore::save_meta(&store.meta, META_LAST_PURGED, &purged).unwrap();
            let mut batch = sled::Batch::default();
            for idx in 3u64..=4 {
                let entry = blank_entry(1, idx);
                batch.insert(
                    &SledLogStore::index_key(idx),
                    SledLogStore::serialize_entry(&entry).unwrap(),
                );
            }
            store.log.apply_batch(batch).unwrap();
            let committed = Some(LogId::new(CommittedLeaderId::new(1, 0), 4));
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_committed(
                &mut store, committed,
            )
            .await
            .unwrap();
            store.db.flush().unwrap();
        }

        let report = SledLogStore::truncate_from(dir.path(), 3).expect("truncate to empty");

        assert_eq!(report.removed_entries, 2);
        assert_eq!(report.new_last_index, None);
        assert_eq!(
            report.committed_after.map(|c| c.index),
            Some(2),
            "with an empty log the committed marker must fall back to last_purged"
        );
    }

    /// The decode error surfaced through openraft's fatal must name the
    /// entry index and the recovery tooling, so the 3 AM operator is not
    /// left with a bare bincode message (per
    /// bug-raft-log-bincode-format-instability item 4).
    #[tokio::test]
    async fn decode_error_names_entry_index_and_recovery_tool() {
        let dir = tempfile::tempdir().unwrap();
        seed_store(dir.path(), 6..=8, &[7]).await;

        let mut store = SledLogStore::new(dir.path()).unwrap();
        let err = store
            .try_get_log_entries(6u64..9u64)
            .await
            .expect_err("entry 7 is undecodable");
        let msg = err.to_string();
        assert!(
            msg.contains("raft log entry 7"),
            "error must name the failing index, got: {msg}"
        );
        assert!(
            msg.contains("ferrosa-ctl raft log-inspect"),
            "error must name the recovery tooling, got: {msg}"
        );
    }

    // -- golden-file decode gate (CI format-drift tripwire) -----------------

    /// Deterministic, representative entry set for the golden fixture.
    /// Covers blank, membership, and Normal payloads across the RaftOp
    /// variants whose embedded types have historically drifted
    /// (NodeInfo, IndexMetadata + FilterPredicate single/conjunction).
    fn golden_entries() -> Vec<Entry<FerrosRaftConfig>> {
        use ferrosa_index::{FilterClause, FilterOp, FilterPredicate, IndexType};

        fn cmd(index: u64, op: RaftOp) -> Entry<FerrosRaftConfig> {
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(3, 1), index),
                payload: EntryPayload::Normal(RaftCommand {
                    op,
                    schema_version: Uuid::from_u128(0xFE01u128 + index as u128),
                }),
            }
        }
        let node = NodeInfo {
            host_id: Uuid::from_u128(0xAA),
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: Some("10.0.0.1:9042".to_string()),
        };
        let single_idx = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_single".into(),
            index_type: IndexType::Filtered,
            target_columns: vec!["status".into()],
            filter_predicate: Some(FilterPredicate::single(0, FilterOp::Eq, b"active".to_vec())),
            options: std::collections::HashMap::new(),
        };
        let conj_idx = IndexMetadata {
            filter_predicate: Some(FilterPredicate::conjunction(vec![
                FilterClause::new(0, FilterOp::Eq, b"active".to_vec()),
                FilterClause::new(2, FilterOp::Gt, b"100".to_vec()),
            ])),
            name: "idx_conj".into(),
            ..single_idx.clone()
        };

        let membership = openraft::Membership::<u64, openraft::BasicNode>::new(
            vec![[1u64, 2, 3].into_iter().collect()],
            [
                (1u64, openraft::BasicNode::new("10.0.0.1:7000")),
                (2u64, openraft::BasicNode::new("10.0.0.2:7000")),
                (3u64, openraft::BasicNode::new("10.0.0.3:7000")),
            ]
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        );

        vec![
            blank_entry(3, 1),
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(3, 1), 2),
                payload: EntryPayload::Membership(membership),
            },
            cmd(3, RaftOp::JoinNode(node.clone())),
            cmd(4, RaftOp::UpdateNodeInfo(node)),
            cmd(
                5,
                RaftOp::AssignTokens {
                    node_id: 42,
                    tokens: vec![-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807],
                },
            ),
            cmd(
                6,
                RaftOp::SetNodeState {
                    node_id: 42,
                    state: NodeState::Leaving,
                },
            ),
            cmd(7, RaftOp::LeaveNode { node_id: 42 }),
            cmd(
                8,
                RaftOp::ApproveNode {
                    host_id: Uuid::from_u128(0xBB),
                },
            ),
            cmd(9, RaftOp::CreateIndex(single_idx)),
            cmd(10, RaftOp::CreateIndex(conj_idx)),
            cmd(
                11,
                RaftOp::DropIndex {
                    keyspace: "ks".into(),
                    table: "tbl".into(),
                    index: "idx_single".into(),
                },
            ),
            cmd(12, RaftOp::DropKeyspace("ks".into())),
            cmd(
                13,
                RaftOp::DropTable {
                    keyspace: "ks".into(),
                    table: "tbl".into(),
                },
            ),
        ]
    }

    /// CI gate: raft log entry bytes written by a previous build of this
    /// crate must decode forever. The fixture freezes serialize_entry
    /// output at the time it was generated; if this test fails, the
    /// current change altered the persisted wire format and WILL brick
    /// existing clusters on upgrade (see
    /// specs/implemented/bug-raft-log-bincode-format-instability.md).
    ///
    /// Regenerate ONLY for an intentional, version-bumped format change:
    /// `FERROSA_REGEN_RAFT_GOLDEN=1 cargo test -p ferrosa-cluster golden_raft_log_entries_decode`
    #[test]
    fn golden_raft_log_entries_decode() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/raft_log_entries_golden_v1.bin");

        let blobs: Vec<Vec<u8>> = if std::env::var_os("FERROSA_REGEN_RAFT_GOLDEN").is_some() {
            let blobs: Vec<Vec<u8>> = golden_entries()
                .iter()
                .map(|e| SledLogStore::serialize_entry(e).unwrap())
                .collect();
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, bincode::serialize(&blobs).unwrap()).unwrap();
            blobs
        } else {
            let raw = fs::read(&path).unwrap_or_else(|e| {
                panic!(
                    "golden raft log fixture missing at {} ({e}); regenerate with \
                     FERROSA_REGEN_RAFT_GOLDEN=1 cargo test -p ferrosa-cluster \
                     golden_raft_log_entries_decode",
                    path.display()
                )
            });
            bincode::deserialize(&raw).expect("golden fixture container must decode")
        };

        let expected = golden_entries();
        assert_eq!(blobs.len(), expected.len(), "fixture entry count drifted");

        for (i, blob) in blobs.iter().enumerate() {
            let entry = SledLogStore::deserialize_entry(blob).unwrap_or_else(|e| {
                panic!(
                    "golden raft log entry {i} (index {}) no longer decodes — this \
                     change altered the persisted raft log wire format and will brick \
                     existing clusters on upgrade. Either restore wire compatibility \
                     or bump ENTRY_FORMAT_VERSION with a migration path. Error: {e}",
                    expected[i].log_id.index
                )
            });
            assert_eq!(
                entry.log_id, expected[i].log_id,
                "golden entry {i} decoded to a different log id"
            );
        }

        // Spot-check the drift-prone embedded types survived.
        let decoded_conj = SledLogStore::deserialize_entry(&blobs[9]).unwrap();
        match decoded_conj.payload {
            EntryPayload::Normal(RaftCommand {
                op: RaftOp::CreateIndex(meta),
                ..
            }) => {
                let pred = meta.filter_predicate.expect("conjunction predicate");
                assert_eq!(pred.clauses().len(), 2, "conjunction clauses must survive");
            }
            other => panic!("golden entry 9 must be CreateIndex, got {other:?}"),
        }
    }
}
