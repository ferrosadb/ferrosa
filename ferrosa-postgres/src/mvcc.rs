//! PostgreSQL-only MVCC snapshots and optimistic serializability validation.
//!
//! CQL transactions are coordinated by Accord. This manager is deliberately
//! separate: it versions PostgreSQL row images and tracks the oldest live
//! PostgreSQL snapshot.
//! Runtime bounds cover transaction write sets, scan buffering, and snapshot
//! retention; malformed environment settings log and use defaults.
//! Last revised: 2026-09-26
//! Last changed: Added startup-configurable scan buffer capacity and snapshot expiry.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_sql::{Row, Value};
use ferrosa_storage::commitlog::Mutation;

pub(crate) const DEFAULT_MAX_TXN_WRITES: usize = 10_000;
pub(crate) const DEFAULT_SCAN_BUFFER_ROWS: usize = 64;
const DEFAULT_MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(600);
const DEFAULT_SNAPSHOT_REAPER_INTERVAL: Duration = Duration::from_secs(1);
const MAX_TXN_WRITES_ENV: &str = "FERROSA_POSTGRES_MAX_TXN_WRITES";
const SCAN_BUFFER_ROWS_ENV: &str = "FERROSA_POSTGRES_SCAN_BUFFER_ROWS";
const MAX_SNAPSHOT_AGE_MS_ENV: &str = "FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS";
const SNAPSHOT_REAPER_INTERVAL_MS_ENV: &str = "FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MvccConfig {
    max_txn_writes: usize,
    scan_buffer_rows: usize,
    max_snapshot_age: Duration,
    snapshot_reaper_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MvccConfigError(String);

impl fmt::Display for MvccConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for MvccConfigError {}

impl Default for MvccConfig {
    fn default() -> Self {
        Self {
            max_txn_writes: DEFAULT_MAX_TXN_WRITES,
            scan_buffer_rows: DEFAULT_SCAN_BUFFER_ROWS,
            max_snapshot_age: DEFAULT_MAX_SNAPSHOT_AGE,
            snapshot_reaper_interval: DEFAULT_SNAPSHOT_REAPER_INTERVAL,
        }
    }
}

impl MvccConfig {
    fn from_overrides(
        max_txn_writes: Option<&str>,
        scan_buffer_rows: Option<&str>,
        max_snapshot_age_ms: Option<&str>,
        snapshot_reaper_interval_ms: Option<&str>,
    ) -> Result<Self, MvccConfigError> {
        let defaults = Self::default();
        let max_txn_writes =
            parse_positive_usize(MAX_TXN_WRITES_ENV, max_txn_writes, defaults.max_txn_writes)?;
        let scan_buffer_rows = parse_positive_usize(
            SCAN_BUFFER_ROWS_ENV,
            scan_buffer_rows,
            defaults.scan_buffer_rows,
        )?;
        let max_snapshot_age = parse_positive_duration(
            MAX_SNAPSHOT_AGE_MS_ENV,
            max_snapshot_age_ms,
            defaults.max_snapshot_age,
        )?;
        let snapshot_reaper_interval = parse_positive_duration(
            SNAPSHOT_REAPER_INTERVAL_MS_ENV,
            snapshot_reaper_interval_ms,
            defaults.snapshot_reaper_interval,
        )?;
        Ok(Self {
            max_txn_writes,
            scan_buffer_rows,
            max_snapshot_age,
            snapshot_reaper_interval,
        })
    }

    fn from_env() -> Self {
        fn read(name: &str) -> Result<Option<String>, MvccConfigError> {
            match env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(env::VarError::NotPresent) => Ok(None),
                Err(error) => Err(MvccConfigError(format!("could not read {name}: {error}"))),
            }
        }

        let overrides = (|| {
            let max_txn_writes = read(MAX_TXN_WRITES_ENV)?;
            let scan_buffer_rows = read(SCAN_BUFFER_ROWS_ENV)?;
            let max_snapshot_age_ms = read(MAX_SNAPSHOT_AGE_MS_ENV)?;
            let snapshot_reaper_interval_ms = read(SNAPSHOT_REAPER_INTERVAL_MS_ENV)?;
            Self::from_overrides(
                max_txn_writes.as_deref(),
                scan_buffer_rows.as_deref(),
                max_snapshot_age_ms.as_deref(),
                snapshot_reaper_interval_ms.as_deref(),
            )
        })();
        match overrides {
            Ok(config) => config,
            Err(error) => {
                tracing::error!(%error, "invalid PostgreSQL MVCC configuration; using defaults");
                Self::default()
            }
        }
    }
}

fn parse_positive_usize(
    name: &str,
    value: Option<&str>,
    default: usize,
) -> Result<usize, MvccConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value
        .parse::<usize>()
        .map_err(|error| MvccConfigError(format!("invalid {name} value {value:?}: {error}")))?;
    if parsed == 0 {
        return Err(MvccConfigError(format!("{name} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_positive_duration(
    name: &str,
    value: Option<&str>,
    default: Duration,
) -> Result<Duration, MvccConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    let millis = value
        .parse::<u64>()
        .map_err(|error| MvccConfigError(format!("invalid {name} value {value:?}: {error}")))?;
    if millis == 0 {
        return Err(MvccConfigError(format!("{name} must be greater than zero")));
    }
    Ok(Duration::from_millis(millis))
}

/// A PostgreSQL transaction's buffered storage mutation. It is applied only
/// by the PostgreSQL MVCC commit path; Cassandra's Accord buffers are separate.
#[derive(Clone, Debug)]
pub struct PgWrite(pub(crate) Mutation);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RowKey {
    table: String,
    key: Vec<Value>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowChange {
    pub table: String,
    pub key: Vec<Value>,
    pub partition_key: Vec<u8>,
    pub before: Option<Row>,
    pub after: Option<Row>,
}

#[derive(Clone, Copy)]
struct ActiveSnapshot {
    read_ts: u64,
    cluster_ts: Option<Timestamp>,
    started_at: Instant,
}

#[derive(Default)]
struct State {
    commit_seq: u64,
    next_snapshot_id: u64,
    active_snapshots: HashMap<u64, ActiveSnapshot>,
    versions: HashMap<RowKey, BTreeMap<u64, Option<Row>>>,
    distributed_versions: HashMap<RowKey, BTreeMap<Timestamp, Option<Row>>>,
    applied_accord_txns: HashSet<(TxnId, Timestamp)>,
    table_epochs: HashMap<String, u64>,
}

/// Shared PostgreSQL version history. History stores only rows changed through
/// PostgreSQL; CQL mutations remain on their existing Accord path.
pub struct MvccManager {
    state: Arc<Mutex<State>>,
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    config: MvccConfig,
}

impl Default for MvccManager {
    fn default() -> Self {
        Self::with_config(MvccConfig::default())
    }
}

#[derive(Clone)]
pub(crate) struct MvccSnapshot {
    id: u64,
    read_ts: u64,
    cluster_ts: Option<Timestamp>,
    _lease: Arc<SnapshotLease>,
}

impl MvccSnapshot {
    pub(crate) fn cluster_timestamp(&self) -> Option<Timestamp> {
        self.cluster_ts
    }
}

struct SnapshotLease {
    id: u64,
    state: Weak<Mutex<State>>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            if let Ok(mut state) = state.lock() {
                state.active_snapshots.remove(&self.id);
                prune_versions(&mut state);
            }
        }
    }
}

impl MvccManager {
    /// Build a manager using validated runtime environment settings. Invalid
    /// values are logged and the full default set is used.
    pub fn from_env() -> Self {
        Self::with_config(MvccConfig::from_env())
    }

    fn with_config(config: MvccConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
            config,
        }
    }

    pub(crate) fn max_txn_writes(&self) -> usize {
        self.config.max_txn_writes
    }

    pub(crate) fn scan_buffer_rows(&self) -> usize {
        self.config.scan_buffer_rows
    }

    #[cfg(test)]
    pub(crate) fn with_max_txn_writes(max_txn_writes: usize) -> Self {
        Self::with_config(MvccConfig {
            max_txn_writes,
            ..MvccConfig::default()
        })
    }

    pub(crate) fn spawn_snapshot_reaper(manager: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(manager.config.snapshot_reaper_interval);
            loop {
                interval.tick().await;
                let expired = manager.expire_snapshots_before(Instant::now());
                if expired > 0 {
                    tracing::warn!(
                        expired,
                        "expired PostgreSQL MVCC snapshot(s) past maximum age"
                    );
                }
            }
        })
    }

    fn expire_snapshots_before(&self, now: Instant) -> usize {
        let mut state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        let max_age = self.config.max_snapshot_age;
        let before = state.active_snapshots.len();
        state
            .active_snapshots
            .retain(|_, snapshot| now.saturating_duration_since(snapshot.started_at) < max_age);
        let expired = before - state.active_snapshots.len();
        if expired > 0 {
            prune_versions(&mut state);
        }
        expired
    }

    #[cfg(test)]
    pub(crate) fn expire_all_snapshots_for_test(&self) -> usize {
        self.expire_snapshots_before(
            Instant::now() + self.config.max_snapshot_age + Duration::from_millis(1),
        )
    }

    /// Serialize PostgreSQL commit orchestration on this node while a
    /// distributed commit is in flight. Accord supplies the cross-node order.
    pub(crate) async fn commit_guard(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.commit_gate.clone().lock_owned().await
    }

    pub(crate) fn snapshot(&self) -> MvccSnapshot {
        self.snapshot_at(None)
    }

    pub(crate) fn snapshot_with_cluster_ts(&self, cluster_ts: Timestamp) -> MvccSnapshot {
        self.snapshot_at(Some(cluster_ts))
    }

    fn snapshot_at(&self, cluster_ts: Option<Timestamp>) -> MvccSnapshot {
        let mut state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        state.next_snapshot_id = state.next_snapshot_id.wrapping_add(1).max(1);
        let id = state.next_snapshot_id;
        let read_ts = state.commit_seq;
        state.active_snapshots.insert(
            id,
            ActiveSnapshot {
                read_ts,
                cluster_ts,
                started_at: Instant::now(),
            },
        );
        MvccSnapshot {
            id,
            read_ts,
            cluster_ts,
            _lease: Arc::new(SnapshotLease {
                id,
                state: Arc::downgrade(&self.state),
            }),
        }
    }

    pub(crate) fn current_commit_seq(&self) -> u64 {
        self.state
            .lock()
            .expect("PostgreSQL MVCC state poisoned")
            .commit_seq
    }

    pub(crate) fn validate_commit(
        &self,
        snapshot: &MvccSnapshot,
        read_tables: &HashSet<String>,
    ) -> Result<(), MvccCommitError> {
        let state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        validate_snapshot(&state, snapshot, read_tables)
    }

    /// Returns the versioned rows for one table at a snapshot. The storage scan
    /// uses this sparse overlay to replace changed rows and restore deleted rows
    /// without copying the table into each transaction.
    pub(crate) fn table_overlay(
        &self,
        snapshot: &MvccSnapshot,
        table: &str,
    ) -> HashMap<Vec<Value>, Option<Row>> {
        let state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        // Pre-size the returned overlay to the exact number of changed keys for
        // this table, so its backing table is allocated ONCE instead of growing
        // (and reallocating/rehashing) as keys are inserted. The overlay stays
        // sparse — bounded by MVCC-changed keys, never by table size. The
        // per-key `row.clone()` transfers a versioned row out of the shared
        // state under the lock; a sparse overlay of that many owned Rows is the
        // contract and is not a table-scale copy.
        if let Some(cluster_ts) = snapshot.cluster_ts {
            let mut overlay = HashMap::with_capacity(
                state
                    .distributed_versions
                    .keys()
                    .filter(|key| key.table == table)
                    .count(),
            );
            for (key, versions) in &state.distributed_versions {
                if key.table != table {
                    continue;
                }
                if let Some((_, row)) = versions.range(..=cluster_ts).next_back() {
                    overlay.insert(key.key.clone(), row.clone());
                }
            }
            return overlay;
        }
        let mut overlay = HashMap::with_capacity(
            state
                .versions
                .keys()
                .filter(|key| key.table == table)
                .count(),
        );
        for (key, versions) in &state.versions {
            if key.table != table {
                continue;
            }
            if let Some((_, row)) = versions.range(..=snapshot.read_ts).next_back() {
                overlay.insert(key.key.clone(), row.clone());
            }
        }
        overlay
    }

    /// Applies a PostgreSQL commit while holding the PG commit-order lock.
    /// Table-level read validation conservatively catches both row conflicts
    /// and predicate phantoms. `apply` must make the write batch atomic.
    pub(crate) fn commit(
        &self,
        snapshot: &MvccSnapshot,
        read_tables: &HashSet<String>,
        apply: impl FnOnce() -> Result<Vec<RowChange>, String>,
    ) -> Result<u64, MvccCommitError> {
        let mut state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        validate_snapshot(&state, snapshot, read_tables)?;
        let changes = apply().map_err(MvccCommitError::Storage)?;
        if changes.is_empty() {
            return Ok(state.commit_seq);
        }
        let previous_commit_seq = state.commit_seq;
        let commit_seq = state.commit_seq.saturating_add(1);
        for change in &changes {
            let key = RowKey {
                table: change.table.clone(),
                key: change.key.clone(),
            };
            let versions = state.versions.entry(key).or_default();
            if versions.is_empty() {
                versions.insert(previous_commit_seq, change.before.clone());
            }
            versions.insert(commit_seq, change.after.clone());
            state.table_epochs.insert(change.table.clone(), commit_seq);
        }
        state.commit_seq = commit_seq;
        prune_versions(&mut state);
        Ok(commit_seq)
    }

    fn record_applied_accord_commit(
        &self,
        txn_id: TxnId,
        timestamp: Timestamp,
        changes: Vec<RowChange>,
    ) -> u64 {
        let mut state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        if changes.is_empty() || !state.applied_accord_txns.insert((txn_id, timestamp)) {
            return state.commit_seq;
        }
        let commit_seq = state.commit_seq.saturating_add(1);
        for change in changes {
            let key = RowKey {
                table: change.table.clone(),
                key: change.key,
            };
            let versions = state.distributed_versions.entry(key).or_default();
            // `prepare` inserted these row images before the storage batch so
            // scans racing apply can safely retain their Accord snapshot. Keep
            // these inserts idempotent in case a custom applier skips prepare.
            let baseline = Timestamp {
                epoch: 0,
                time: 0,
                seq: 0,
                node: 0,
            };
            if versions.is_empty() {
                versions.insert(baseline, change.before);
            }
            versions.entry(timestamp).or_insert(change.after);
            state.table_epochs.insert(change.table, commit_seq);
        }
        state.commit_seq = commit_seq;
        prune_versions(&mut state);
        commit_seq
    }

    #[cfg(test)]
    pub(crate) fn active_snapshot_count(&self) -> usize {
        self.state
            .lock()
            .expect("PostgreSQL MVCC state poisoned")
            .active_snapshots
            .len()
    }

    #[cfg(test)]
    fn retained_version_count(&self) -> usize {
        self.state
            .lock()
            .expect("PostgreSQL MVCC state poisoned")
            .versions
            .values()
            .map(BTreeMap::len)
            .sum()
    }
}

impl ferrosa_storage::accord::PostgresMvccApplyObserver for MvccManager {
    fn prepare_postgres_apply(
        &self,
        _txn_id: TxnId,
        timestamp: Timestamp,
        metadata: &[Vec<u8>],
    ) -> Result<(), String> {
        let mut changes = decode_row_changes(metadata)?;
        let mut state = self.state.lock().expect("PostgreSQL MVCC state poisoned");
        let baseline = Timestamp {
            epoch: 0,
            time: 0,
            seq: 0,
            node: 0,
        };
        for change in changes.drain(..) {
            let key = RowKey {
                table: change.table,
                key: change.key,
            };
            let versions = state.distributed_versions.entry(key).or_default();
            if versions.is_empty() {
                versions.insert(baseline, change.before);
            }
            versions.entry(timestamp).or_insert(change.after);
        }
        Ok(())
    }

    fn on_postgres_apply(
        &self,
        txn_id: TxnId,
        timestamp: Timestamp,
        metadata: &[Vec<u8>],
    ) -> Result<(), String> {
        let changes = decode_row_changes(metadata)?;
        self.record_applied_accord_commit(txn_id, timestamp, changes);
        Ok(())
    }
}

fn decode_row_changes(metadata: &[Vec<u8>]) -> Result<Vec<RowChange>, String> {
    let mut changes = Vec::new();
    for payload in metadata {
        let mut partition_changes: Vec<RowChange> = serde_json::from_slice(payload)
            .map_err(|error| format!("decode PostgreSQL MVCC apply metadata: {error}"))?;
        changes.append(&mut partition_changes);
    }
    Ok(changes)
}

fn validate_snapshot(
    state: &State,
    snapshot: &MvccSnapshot,
    read_tables: &HashSet<String>,
) -> Result<(), MvccCommitError> {
    if !state.active_snapshots.contains_key(&snapshot.id) {
        return Err(MvccCommitError::SnapshotExpired);
    }
    if read_tables.iter().any(|table| {
        state
            .table_epochs
            .get(table)
            .is_some_and(|epoch| *epoch > snapshot.read_ts)
    }) {
        return Err(MvccCommitError::SerializationFailure);
    }
    Ok(())
}

fn prune_versions(state: &mut State) {
    let oldest_live = state
        .active_snapshots
        .values()
        .map(|snapshot| snapshot.read_ts)
        .min();
    state
        .versions
        .retain(|_, versions| retain_snapshot_history(versions, oldest_live));

    let oldest_cluster = state
        .active_snapshots
        .values()
        .filter_map(|snapshot| snapshot.cluster_ts)
        .min();
    state
        .distributed_versions
        .retain(|_, versions| retain_snapshot_history(versions, oldest_cluster));
}

/// Keep the version visible at the oldest active snapshot and every later
/// version. Move rows between BTreeMaps so pruning does not copy row payloads.
fn retain_snapshot_history<K: Copy + Ord, V>(
    versions: &mut BTreeMap<K, V>,
    oldest_snapshot: Option<K>,
) -> bool {
    if let Some(oldest_snapshot) = oldest_snapshot {
        let mut retained = versions.split_off(&oldest_snapshot);
        if let Some((timestamp, row)) = versions.pop_last() {
            retained.insert(timestamp, row);
        }
        *versions = retained;
    } else if let Some(timestamp) = versions.last_key_value().map(|(timestamp, _)| *timestamp) {
        let latest = versions
            .remove(&timestamp)
            .expect("timestamp came from the same version map");
        versions.clear();
        versions.insert(timestamp, latest);
    }
    !versions.is_empty()
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MvccCommitError {
    SerializationFailure,
    SnapshotExpired,
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn row(value: &str) -> Row {
        Row::new(vec![Value::Text(value.to_string())])
    }

    fn assert_snapshot_row(
        manager: &MvccManager,
        snapshot: &MvccSnapshot,
        table: &str,
        key: &[Value],
        expected: Option<Row>,
    ) {
        let state = manager
            .state
            .lock()
            .expect("PostgreSQL MVCC state poisoned");
        let actual = state
            .versions
            .get(&RowKey {
                table: table.to_string(),
                key: key.to_vec(),
            })
            .and_then(|versions| versions.range(..=snapshot.read_ts).next_back())
            .map(|(_, row)| row);
        assert_eq!(actual, Some(&expected));
    }

    fn change(before: Option<Row>, after: Option<Row>) -> RowChange {
        RowChange {
            table: "public.items".to_string(),
            key: vec![Value::Int(1)],
            partition_key: vec![1],
            before,
            after,
        }
    }

    #[test]
    fn snapshot_reads_the_version_visible_at_begin() {
        let manager = Arc::new(MvccManager::default());
        let initial = manager.snapshot();
        manager
            .commit(&initial, &HashSet::new(), || {
                Ok(vec![change(None, Some(row("before")))])
            })
            .unwrap();
        let reader = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(&writer, &HashSet::new(), || {
                Ok(vec![change(Some(row("before")), Some(row("after")))])
            })
            .unwrap();

        assert_snapshot_row(
            &manager,
            &reader,
            "public.items",
            &[Value::Int(1)],
            Some(row("before")),
        );
    }

    #[test]
    fn read_predicate_changed_after_snapshot_rejects_commit() {
        let manager = Arc::new(MvccManager::default());
        let reader = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(&writer, &HashSet::new(), || {
                Ok(vec![change(None, Some(row("new")))])
            })
            .unwrap();

        let result = manager.commit(
            &reader,
            &HashSet::from(["public.items".to_string()]),
            || Ok(Vec::new()),
        );
        assert_eq!(result, Err(MvccCommitError::SerializationFailure));
    }

    #[test]
    fn dropping_snapshot_releases_it_from_retention_tracking() {
        let manager = Arc::new(MvccManager::default());
        let snapshot = manager.snapshot();
        assert_eq!(manager.active_snapshot_count(), 1);
        drop(snapshot);
        assert_eq!(manager.active_snapshot_count(), 0);
    }

    #[test]
    fn version_gc_retains_oldest_live_snapshot_then_reclaims_history() {
        let manager = Arc::new(MvccManager::default());
        let setup = manager.snapshot();
        manager
            .commit(&setup, &HashSet::new(), || {
                Ok(vec![change(None, Some(row("v1")))])
            })
            .unwrap();
        drop(setup);

        let reader = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(&writer, &HashSet::new(), || {
                Ok(vec![change(Some(row("v1")), Some(row("v2")))])
            })
            .unwrap();
        assert_eq!(manager.retained_version_count(), 2);
        assert_snapshot_row(
            &manager,
            &reader,
            "public.items",
            &[Value::Int(1)],
            Some(row("v1")),
        );

        drop(reader);
        drop(writer);
        assert_eq!(manager.retained_version_count(), 1);
    }

    #[test]
    fn mvcc_config_defaults_write_cap_and_snapshot_max_age() {
        let config = MvccConfig::from_overrides(None, None, None, None).unwrap();
        assert_eq!(config.max_txn_writes, DEFAULT_MAX_TXN_WRITES);
        assert_eq!(config.scan_buffer_rows, DEFAULT_SCAN_BUFFER_ROWS);
        assert_eq!(config.max_snapshot_age, Duration::from_secs(600));
    }

    #[test]
    fn mvcc_config_accepts_write_cap_and_snapshot_max_age_overrides() {
        let config =
            MvccConfig::from_overrides(Some("25"), Some("128"), Some("30000"), Some("250"))
                .unwrap();
        assert_eq!(config.max_txn_writes, 25);
        assert_eq!(config.scan_buffer_rows, 128);
        assert_eq!(config.max_snapshot_age, Duration::from_secs(30));
        assert_eq!(config.snapshot_reaper_interval, Duration::from_millis(250));
        assert_eq!(MvccManager::with_config(config).scan_buffer_rows(), 128);
    }

    #[test]
    fn mvcc_config_rejects_zero_and_malformed_overrides() {
        assert!(MvccConfig::from_overrides(Some("0"), None, None, None).is_err());
        assert!(MvccConfig::from_overrides(None, Some("0"), None, None).is_err());
        assert!(MvccConfig::from_overrides(None, None, Some("0"), None).is_err());
        assert!(MvccConfig::from_overrides(None, None, None, Some("0")).is_err());
        assert!(MvccConfig::from_overrides(Some("x"), None, None, None).is_err());
        assert!(MvccConfig::from_overrides(None, None, Some("x"), None).is_err());
    }

    #[test]
    fn expiring_an_old_snapshot_releases_history_and_rejects_commit() {
        let config = MvccConfig::from_overrides(None, None, Some("5"), None).unwrap();
        let manager = MvccManager::with_config(config);
        let setup = manager.snapshot();
        manager
            .commit(&setup, &HashSet::new(), || {
                Ok(vec![change(None, Some(row("v1")))])
            })
            .unwrap();
        drop(setup);

        let old_snapshot = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(&writer, &HashSet::new(), || {
                Ok(vec![change(Some(row("v1")), Some(row("v2")))])
            })
            .unwrap();
        drop(writer);
        assert_eq!(manager.retained_version_count(), 2);

        let expired = manager.expire_snapshots_before(Instant::now() + Duration::from_millis(6));
        assert_eq!(expired, 1);
        assert_eq!(manager.active_snapshot_count(), 0);
        assert_eq!(manager.retained_version_count(), 1);
        assert_eq!(
            manager.validate_commit(&old_snapshot, &HashSet::new()),
            Err(MvccCommitError::SnapshotExpired)
        );
    }

    #[test]
    fn staged_multi_row_accord_commit_is_visible_as_one_snapshot_version() {
        let manager = MvccManager::default();
        let before = vec![
            RowChange {
                table: "public.items".to_string(),
                key: vec![Value::Int(1)],
                partition_key: vec![1],
                before: Some(row("left-before")),
                after: Some(row("left-after")),
            },
            RowChange {
                table: "public.items".to_string(),
                key: vec![Value::Int(2)],
                partition_key: vec![2],
                before: Some(row("right-before")),
                after: Some(row("right-after")),
            },
        ];
        let metadata = serde_json::to_vec(&before).unwrap();
        let commit_ts = Timestamp::synthetic(20);
        <MvccManager as ferrosa_storage::accord::PostgresMvccApplyObserver>::prepare_postgres_apply(
            &manager,
            TxnId::new(1, Timestamp::synthetic(21)),
            commit_ts,
            &[metadata],
        )
        .unwrap();

        let old_snapshot = manager.snapshot_at(Some(Timestamp::synthetic(19)));
        let committed_snapshot = manager.snapshot_at(Some(commit_ts));
        let old_rows = manager.table_overlay(&old_snapshot, "public.items");
        let committed_rows = manager.table_overlay(&committed_snapshot, "public.items");

        assert_eq!(
            old_rows.get(&vec![Value::Int(1)]).and_then(Option::as_ref),
            Some(&row("left-before"))
        );
        assert_eq!(
            old_rows.get(&vec![Value::Int(2)]).and_then(Option::as_ref),
            Some(&row("right-before"))
        );
        assert_eq!(
            committed_rows
                .get(&vec![Value::Int(1)])
                .and_then(Option::as_ref),
            Some(&row("left-after"))
        );
        assert_eq!(
            committed_rows
                .get(&vec![Value::Int(2)])
                .and_then(Option::as_ref),
            Some(&row("right-after"))
        );
    }
}
