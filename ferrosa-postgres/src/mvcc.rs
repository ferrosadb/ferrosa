//! PostgreSQL-only MVCC snapshots and optimistic serializability validation.
//!
//! CQL transactions are coordinated by Accord. This manager is deliberately
//! separate: it versions PostgreSQL row images and tracks the oldest live
//! PostgreSQL snapshot.
//! Runtime bounds cover scan buffering and snapshot retention; malformed
//! environment settings log and use defaults. There is deliberately NO bound on
//! a transaction's write count: the front end's write-set SPILLS to disk (see
//! `txn_write_set`), so the retired `FERROSA_POSTGRES_MAX_TXN_WRITES` knob is gone.
//! Last revised: 2026-10-24
//! Last changed: Persistent snapshot roots with per-scope apply reservations.

#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;
use imbl::{HashMap as PersistentMap, HashSet as PersistentSet, OrdMap};
use std::time::{Duration, Instant};

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_sql::{Row, Value};
use ferrosa_storage::commitlog::Mutation;

pub(crate) const DEFAULT_SCAN_BUFFER_ROWS: usize = 64;
const DEFAULT_MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(600);
const DEFAULT_SNAPSHOT_REAPER_INTERVAL: Duration = Duration::from_secs(1);
const SCAN_BUFFER_ROWS_ENV: &str = "FERROSA_POSTGRES_SCAN_BUFFER_ROWS";
const MAX_SNAPSHOT_AGE_MS_ENV: &str = "FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS";
const SNAPSHOT_REAPER_INTERVAL_MS_ENV: &str = "FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS";
/// Enables per-`COMMIT` phase timing and resident-history accounting. Off by
/// default: the counters are relaxed atomics, but a disabled run must not pay
/// even that on the hot path.
const COMMIT_PROFILE_ENV: &str = "FERROSA_PG_COMMIT_PROFILE";

/// Per-phase wall-clock and resident-history accounting for the PostgreSQL MVCC
/// apply seam. These answer, with evidence rather than a guess, how a large
/// transaction's commit cost and resident memory split between the four
/// candidates: the version-history insert itself, distributed (Accord)
/// registration, the apply fan-out, and row decode/encode.
#[derive(Default)]
pub(crate) struct MvccProfile {
    enabled: AtomicBool,
    /// `prepare_postgres_apply` — decode metadata + insert history.
    prepare_calls: AtomicU64,
    prepare_ns: AtomicU64,
    prepare_decoded: AtomicU64,
    /// `record_applied_accord_commit` — decode metadata + insert history.
    apply_calls: AtomicU64,
    apply_ns: AtomicU64,
    apply_decoded: AtomicU64,
    /// Time spent inside `prune_versions`.
    prune_calls: AtomicU64,
    prune_ns: AtomicU64,
    /// Versions inserted into either history map (keyed insert of a cloned Row).
    versions_inserted: AtomicU64,
}

impl MvccProfile {
    fn from_env() -> Self {
        let profile = Self::default();
        profile
            .enabled
            .store(env::var_os(COMMIT_PROFILE_ENV).is_some(), Ordering::Relaxed);
        profile
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn add(counter: &AtomicU64, nanos: u64) {
        counter.fetch_add(nanos, Ordering::Relaxed);
    }
}

/// Resident byte accounting for the two MVCC history maps, plus the entry
/// counts needed to read the numbers as a per-key cost. The byte figure is a
/// deterministic ESTIMATE of row payloads and logical key bytes (not HAMT
/// structural overhead or an allocator probe): it is meant to compare before/after and to separate MVCC history
/// residency from the rest of the process, not to be byte-exact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MvccHistoryStats {
    pub local_keys: usize,
    pub local_versions: usize,
    pub local_bytes: usize,
    pub distributed_keys: usize,
    pub distributed_versions: usize,
    pub distributed_bytes: usize,
    pub applied_accord_txns: usize,
}

impl MvccHistoryStats {
    pub(crate) fn total_keys(&self) -> usize {
        self.local_keys + self.distributed_keys
    }

    pub(crate) fn total_versions(&self) -> usize {
        self.local_versions + self.distributed_versions
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.local_bytes + self.distributed_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MvccConfig {
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
            scan_buffer_rows: DEFAULT_SCAN_BUFFER_ROWS,
            max_snapshot_age: DEFAULT_MAX_SNAPSHOT_AGE,
            snapshot_reaper_interval: DEFAULT_SNAPSHOT_REAPER_INTERVAL,
        }
    }
}

impl MvccConfig {
    fn from_overrides(
        scan_buffer_rows: Option<&str>,
        max_snapshot_age_ms: Option<&str>,
        snapshot_reaper_interval_ms: Option<&str>,
    ) -> Result<Self, MvccConfigError> {
        let defaults = Self::default();
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
            let scan_buffer_rows = read(SCAN_BUFFER_ROWS_ENV)?;
            let max_snapshot_age_ms = read(MAX_SNAPSHOT_AGE_MS_ENV)?;
            let snapshot_reaper_interval_ms = read(SNAPSHOT_REAPER_INTERVAL_MS_ENV)?;
            Self::from_overrides(
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
pub(crate) struct RowKey {
    pub(crate) table: String,
    pub(crate) key: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowChange {
    pub table: String,
    pub key: Vec<Value>,
    pub partition_key: Vec<u8>,
    pub before: Option<Row>,
    pub after: Option<Row>,
}

// Immutable HAMT paths and row payloads are structurally shared. A snapshot
// owns its root through the registry; expiry drops ownership without a sweep.
type SharedRow = Arc<Option<Row>>;
type TableRows<V> = PersistentMap<String, PersistentMap<Arc<Vec<Value>>, V>>;
type LocalRows = TableRows<LocalVersion>;
type ClusterRows = TableRows<OrdMap<Timestamp, SharedRow>>;

#[derive(Clone)]
struct LocalVersion {
    seq: u64,
    row: SharedRow,
}
#[derive(Clone)]
struct ActiveSnapshot {
    cluster_ts: Option<Timestamp>,
    started_at: Instant,
    local: LocalRows,
    cluster: ClusterRows,
    reads: PersistentSet<RowKey>,
}
#[derive(Clone)]
struct Reservation {
    writes: PersistentSet<RowKey>,
    reads: PersistentSet<RowKey>,
    tables: HashSet<String>,
}
#[derive(Clone, Default)]
struct State {
    commit_seq: u64,
    next_snapshot_id: u64,
    active_snapshots: PersistentMap<u64, ActiveSnapshot>,
    local: LocalRows,
    cluster: ClusterRows,
    cluster_applied: TableRows<Timestamp>,
    keys: TableRows<()>,
    reservations: PersistentMap<u64, Reservation>,
    applied_accord_txns: PersistentSet<(TxnId, Timestamp)>,
    table_epochs: PersistentMap<String, u64>,
}

/// Atomic persistent metadata publication. Physical storage/WAL work remains
/// external, with no metadata mutex or global pending owner held across it.
pub struct MvccManager {
    state: Arc<ArcSwap<State>>,
    next_reservation: AtomicU64,
    config: MvccConfig,
    profile: Arc<MvccProfile>,
}

#[cfg(test)]
thread_local! {
    static BEFORE_CAS: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static AFTER_LIVENESS: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static CAS_RETRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Retry pure metadata only. Never invoke storage or a user callback here.
fn update<R>(state: &ArcSwap<State>, f: impl Fn(&mut State) -> R) -> R {
    loop {
        let old = state.load_full();
        let mut next = (*old).clone();
        let result = f(&mut next);
        #[cfg(test)]
        {
            let hook = BEFORE_CAS.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }
        let previous = state.compare_and_swap(&old, Arc::new(next));
        if Arc::ptr_eq(&old, &previous) {
            return result;
        }
        #[cfg(test)]
        CAS_RETRIES.with(|retries| retries.set(retries.get() + 1));
    }
}

struct ApplyReservation<'a> {
    manager: &'a MvccManager,
    id: u64,
    armed: bool,
}
impl Drop for ApplyReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            update(&self.manager.state, |state| {
                state.reservations.remove(&self.id);
            });
        }
    }
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
    state: Weak<ArcSwap<State>>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            update(&state, |state| {
                state.active_snapshots.remove(&self.id);
            });
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
            state: Arc::new(ArcSwap::from_pointee(State::default())),
            next_reservation: AtomicU64::new(1),
            config,
            profile: Arc::new(MvccProfile::from_env()),
        }
    }

    /// Resident-history accounting: entry counts and an estimated byte cost for
    /// both version maps. Used to attribute the resident term of a large commit
    /// and to prove the size bound holds.
    pub(crate) fn history_stats(&self) -> MvccHistoryStats {
        let state = self.state.load();
        history_stats(&state)
    }

    /// Cumulative per-phase timings since process start, as `(label, calls,
    /// nanos)` triples. Empty unless [`COMMIT_PROFILE_ENV`] was set.
    pub(crate) fn profile_report(&self) -> Vec<(&'static str, u64, u64)> {
        let profile = &self.profile;
        vec![
            (
                "mvcc.prepare_postgres_apply",
                profile.prepare_calls.load(Ordering::Relaxed),
                profile.prepare_ns.load(Ordering::Relaxed),
            ),
            (
                "mvcc.on_postgres_apply",
                profile.apply_calls.load(Ordering::Relaxed),
                profile.apply_ns.load(Ordering::Relaxed),
            ),
            (
                "mvcc.prune_versions",
                profile.prune_calls.load(Ordering::Relaxed),
                profile.prune_ns.load(Ordering::Relaxed),
            ),
        ]
    }

    /// Number of decoded `RowChange`s seen by the apply observer (the row
    /// decode/encode volume the commit pays for).
    pub(crate) fn profile_decoded(&self) -> u64 {
        self.profile
            .prepare_decoded
            .load(Ordering::Relaxed)
            .saturating_add(self.profile.apply_decoded.load(Ordering::Relaxed))
    }

    /// Number of row versions inserted into either history map.
    pub(crate) fn profile_versions_inserted(&self) -> u64 {
        self.profile.versions_inserted.load(Ordering::Relaxed)
    }

    pub(crate) fn scan_buffer_rows(&self) -> usize {
        self.config.scan_buffer_rows
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
        update(&self.state, |state| {
            let expired: Vec<_> = state
                .active_snapshots
                .iter()
                .filter(|(_, snapshot)| {
                    now.saturating_duration_since(snapshot.started_at)
                        >= self.config.max_snapshot_age
                })
                .map(|(id, _)| *id)
                .collect();
            for id in &expired {
                state.active_snapshots.remove(id);
            }
            expired.len()
        })
    }

    #[cfg(test)]
    pub(crate) fn expire_all_snapshots_for_test(&self) -> usize {
        self.expire_snapshots_before(
            Instant::now() + self.config.max_snapshot_age + Duration::from_millis(1),
        )
    }

    pub(crate) fn snapshot(&self) -> MvccSnapshot {
        self.snapshot_at(None)
    }

    pub(crate) fn snapshot_with_cluster_ts(&self, cluster_ts: Timestamp) -> MvccSnapshot {
        self.snapshot_at(Some(cluster_ts))
    }

    fn snapshot_at(&self, cluster_ts: Option<Timestamp>) -> MvccSnapshot {
        let (id, read_ts) = update(&self.state, |state| {
            state.next_snapshot_id = state.next_snapshot_id.wrapping_add(1).max(1);
            let id = state.next_snapshot_id;
            state.active_snapshots.insert(
                id,
                ActiveSnapshot {
                    cluster_ts,
                    started_at: Instant::now(),
                    local: state.local.clone(),
                    cluster: state.cluster.clone(),
                    reads: PersistentSet::new(),
                },
            );
            (id, state.commit_seq)
        });
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
        self.state.load().commit_seq
    }

    pub(crate) fn validate_commit(
        &self,
        snapshot: &MvccSnapshot,
        read_tables: &HashSet<String>,
    ) -> Result<(), MvccCommitError> {
        let state = self.state.load();
        validate_snapshot(&state, snapshot, read_tables)
    }

    /// Whether `snapshot` is still live in this manager. The pre-statement
    /// liveness probe uses this, NOT [`Self::validate_commit`]: a read-set
    /// conflict is a COMMIT-time decision, and must not abort a later read on a
    /// still-valid snapshot (which must keep reading from its BEGIN snapshot).
    pub(crate) fn snapshot_is_active(&self, snapshot: &MvccSnapshot) -> bool {
        let state = self.state.load();
        let active = state.active_snapshots.contains_key(&snapshot.id);
        #[cfg(test)]
        {
            let hook = AFTER_LIVENESS.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }
        active
    }

    #[cfg(test)]
    pub(crate) fn expire_after_liveness_for_test(manager: Arc<Self>) {
        AFTER_LIVENESS.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                manager.expire_all_snapshots_for_test();
            }))
        });
    }

    /// Record a row read BY KEY under `snapshot`, so commit judges that read at
    /// row granularity instead of by table identity. A snapshot that has already
    /// left `active_snapshots` records nothing: its transaction is over and the
    /// entry would be pruned anyway. Called only for an exact-key point read on
    /// a table's partition key (see `server::track_select_reads`).
    pub(crate) fn record_point_read(&self, snapshot: &MvccSnapshot, row: RowKey) {
        update(&self.state, |state| {
            if let Some(active) = state.active_snapshots.get_mut(&snapshot.id) {
                active.reads.insert(row.clone());
            }
        });
    }

    pub(crate) fn table_view(&self, snapshot: &MvccSnapshot, table: &str) -> MvccTableView {
        MvccTableView {
            state: self.state.clone(),
            snapshot: snapshot.clone(),
            table: table.to_owned(),
        }
    }

    /// Value-assertion convenience over the real production view iterator.
    #[cfg(test)]
    pub(crate) fn table_overlay(
        &self,
        snapshot: &MvccSnapshot,
        table: &str,
    ) -> Result<HashMap<Vec<Value>, Option<Row>>, MvccCommitError> {
        self.table_view(snapshot, table)
            .rows()?
            .map(|image| {
                let (key, row) = image?;
                Ok(((*key).clone(), (*row).clone()))
            })
            .collect()
    }

    /// Reserve scopes and publish before-images before the single storage effect.
    pub(crate) fn commit(
        &self,
        snapshot: &MvccSnapshot,
        read_tables: &HashSet<String>,
        changes: Vec<RowChange>,
        apply: impl FnOnce() -> Result<(), ferrosa_common::Error>,
    ) -> Result<u64, MvccCommitError> {
        let id = self.next_reservation.fetch_add(1, Ordering::Relaxed);
        let prepared: Vec<_> = changes
            .into_iter()
            .map(|change| {
                (
                    RowKey {
                        table: change.table.clone(),
                        key: change.key.clone(),
                    },
                    Arc::new(change.key),
                    Arc::new(change.before),
                    Arc::new(change.after),
                )
            })
            .collect();
        update(&self.state, |state| {
            validate_snapshot(state, snapshot, read_tables)?;
            let request = Reservation {
                writes: prepared.iter().map(|(key, ..)| key.clone()).collect(),
                reads: state.active_snapshots[&snapshot.id].reads.clone(),
                tables: read_tables.clone(),
            };
            for (key, ..) in &prepared {
                if row_changed(state, snapshot, key) {
                    return Err(MvccCommitError::SerializationFailure);
                }
            }
            if state
                .reservations
                .values()
                .any(|other| reservations_conflict(&request, other))
            {
                return Err(MvccCommitError::SerializationFailure);
            }
            for (key, owned_key, before, _) in &prepared {
                state
                    .keys
                    .entry(key.table.clone())
                    .or_default()
                    .insert(owned_key.clone(), ());
                // A first-touch baseline has never been committed by MVCC.
                // Inheriting an unrelated global epoch would manufacture a
                // conflict even when physical apply later fails.
                let seq = 0;
                state
                    .local
                    .entry(key.table.clone())
                    .or_default()
                    .entry(owned_key.clone())
                    .or_insert_with(|| LocalVersion {
                        seq,
                        row: before.clone(),
                    });
                // Only changed paths, never unrelated row histories.
                for (_, active) in state.active_snapshots.iter_mut() {
                    active
                        .local
                        .entry(key.table.clone())
                        .or_default()
                        .entry(owned_key.clone())
                        .or_insert_with(|| LocalVersion {
                            seq,
                            row: before.clone(),
                        });
                }
            }
            state.reservations.insert(id, request);
            Ok(())
        })?;
        let mut reservation = ApplyReservation {
            manager: self,
            id,
            armed: true,
        };
        apply().map_err(MvccCommitError::Storage)?;
        let seq = update(&self.state, |state| {
            if !prepared.is_empty() {
                state.commit_seq = state.commit_seq.saturating_add(1);
                for (key, owned_key, _, after) in &prepared {
                    state.local.entry(key.table.clone()).or_default().insert(
                        owned_key.clone(),
                        LocalVersion {
                            seq: state.commit_seq,
                            row: after.clone(),
                        },
                    );
                    state
                        .table_epochs
                        .insert(key.table.clone(), state.commit_seq);
                }
            }
            state.reservations.remove(&id);
            state.commit_seq
        });
        reservation.armed = false;
        drop(reservation);
        if self.profile.is_enabled() {
            MvccProfile::add(&self.profile.versions_inserted, prepared.len() as u64);
        }
        Ok(seq)
    }

    fn record_applied_accord_commit(
        &self,
        txn_id: TxnId,
        timestamp: Timestamp,
        changes: Vec<RowChange>,
    ) -> u64 {
        let images = cluster_images(changes);
        let (seq, inserted) = update(&self.state, |state| {
            if images.is_empty() || state.applied_accord_txns.contains(&(txn_id, timestamp)) {
                return (state.commit_seq, false);
            }
            publish_cluster_images(state, timestamp, &images);
            state.applied_accord_txns.insert((txn_id, timestamp));
            state.commit_seq = state.commit_seq.saturating_add(1);
            for (table, key, _, _) in &images {
                state.table_epochs.insert(table.clone(), state.commit_seq);
                let applied = state
                    .cluster_applied
                    .entry(table.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_insert(timestamp);
                *applied = (*applied).max(timestamp);
                let latest = *applied;
                // Compact this head only; active roots own older versions.
                let versions = state.cluster.get_mut(table).unwrap().get_mut(key).unwrap();
                *versions = versions
                    .range(latest..)
                    .map(|(ts, row)| (*ts, row.clone()))
                    .collect();
            }
            (state.commit_seq, true)
        });
        if inserted && self.profile.is_enabled() {
            MvccProfile::add(&self.profile.versions_inserted, images.len() as u64);
        }
        seq
    }

    #[cfg(test)]
    pub(crate) fn count_allocations_for_test(f: impl FnOnce()) -> usize {
        tests::allocation_probe::count(f)
    }

    #[cfg(test)]
    pub(crate) fn active_snapshot_count(&self) -> usize {
        self.state.load().active_snapshots.len()
    }
    #[cfg(test)]
    fn retained_version_count(&self) -> usize {
        self.history_stats().local_versions
    }
}

#[derive(Clone)]
pub(crate) struct MvccTableView {
    state: Arc<ArcSwap<State>>,
    snapshot: MvccSnapshot,
    table: String,
}
impl MvccTableView {
    pub(crate) fn ensure_active(&self) -> Result<(), MvccCommitError> {
        if self
            .state
            .load()
            .active_snapshots
            .contains_key(&self.snapshot.id)
        {
            Ok(())
        } else {
            Err(MvccCommitError::SnapshotExpired)
        }
    }

    pub(crate) fn row(&self, key: &Vec<Value>) -> Result<Option<SharedRow>, MvccCommitError> {
        let state = self.state.load();
        let active = state
            .active_snapshots
            .get(&self.snapshot.id)
            .ok_or(MvccCommitError::SnapshotExpired)?;
        if let Some(ts) = active.cluster_ts {
            match active
                .cluster
                .get(&self.table)
                .and_then(|table| table.get(key))
            {
                Some(versions) => versions
                    .range(..=ts)
                    .next_back()
                    .map(|(_, row)| Some(row.clone()))
                    .ok_or(MvccCommitError::SnapshotExpired),
                None => Ok(None),
            }
        } else {
            Ok(active
                .local
                .get(&self.table)
                .and_then(|table| table.get(key))
                .map(|version| version.row.clone()))
        }
    }
    /// Enumerate a persistent KEY-ONLY root. Each payload is resolved from the
    /// current active registry just before yielding. An unpolled iterator pins
    /// no historical row root, so expiry frees payloads even while it is held.
    pub(crate) fn rows(&self) -> Result<SnapshotRows, MvccCommitError> {
        self.ensure_active()?;
        let keys = self
            .state
            .load()
            .keys
            .get(&self.table)
            .cloned()
            .unwrap_or_default();
        let view = self.clone();
        Ok(Box::new(keys.into_iter().filter_map(
            move |(key, ())| match view.row(key.as_ref()) {
                Ok(Some(row)) => Some(Ok((key, row))),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
        )))
    }
}

type SnapshotRows =
    Box<dyn Iterator<Item = Result<(Arc<Vec<Value>>, SharedRow), MvccCommitError>> + Send>;

type ClusterImage = (String, Arc<Vec<Value>>, SharedRow, SharedRow);
fn cluster_images(changes: Vec<RowChange>) -> Vec<ClusterImage> {
    changes
        .into_iter()
        .map(|c| {
            (
                c.table,
                Arc::new(c.key),
                Arc::new(c.before),
                Arc::new(c.after),
            )
        })
        .collect()
}
fn publish_cluster_images(state: &mut State, timestamp: Timestamp, images: &[ClusterImage]) {
    let baseline = Timestamp::synthetic(0);
    for (table, key, before, after) in images {
        state
            .keys
            .entry(table.clone())
            .or_default()
            .insert(key.clone(), ());
        let publish = |root: &mut ClusterRows| {
            let versions = root
                .entry(table.clone())
                .or_default()
                .entry(key.clone())
                .or_default();
            if versions.is_empty() {
                versions.insert(baseline, before.clone());
            }
            if !versions.contains_key(&timestamp) {
                versions.insert(timestamp, after.clone());
            }
        };
        publish(&mut state.cluster);
        for (_, active) in state.active_snapshots.iter_mut() {
            if active.cluster_ts.is_some() {
                publish(&mut active.cluster);
            }
        }
    }
}
fn reservations_conflict(a: &Reservation, b: &Reservation) -> bool {
    a.writes
        .iter()
        .any(|key| b.writes.contains(key) || b.reads.contains(key) || b.tables.contains(&key.table))
        || b.writes
            .iter()
            .any(|key| a.reads.contains(key) || a.tables.contains(&key.table))
}

impl ferrosa_storage::accord::PostgresMvccApplyObserver for MvccManager {
    fn prepare_postgres_apply(
        &self,
        txn_id: TxnId,
        timestamp: Timestamp,
        metadata: &[Vec<u8>],
    ) -> Result<(), String> {
        let started = self.profile.is_enabled().then(Instant::now);
        let changes = decode_row_changes(metadata)?;
        let decoded = changes.len();
        let images = cluster_images(changes);
        update(&self.state, |state| {
            if !state.applied_accord_txns.contains(&(txn_id, timestamp)) {
                publish_cluster_images(state, timestamp, &images);
            }
        });
        if let Some(started) = started {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            MvccProfile::add(&self.profile.prepare_calls, 1);
            MvccProfile::add(&self.profile.prepare_ns, elapsed);
            MvccProfile::add(
                &self.profile.prepare_decoded,
                u64::try_from(decoded).unwrap_or(u64::MAX),
            );
            if ferrosa_common::mem_probe::installed() {
                tracing::info!(
                    phase = "mvcc.prepare_postgres_apply",
                    decoded,
                    live_mib = ferrosa_common::mem_probe::live_mib(),
                    "pg commit residency"
                );
            }
        }
        Ok(())
    }

    fn on_postgres_apply(
        &self,
        txn_id: TxnId,
        timestamp: Timestamp,
        metadata: &[Vec<u8>],
    ) -> Result<(), String> {
        let started = self.profile.is_enabled().then(Instant::now);
        let changes = decode_row_changes(metadata)?;
        let decoded = changes.len();
        self.record_applied_accord_commit(txn_id, timestamp, changes);
        if let Some(started) = started {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            MvccProfile::add(&self.profile.apply_calls, 1);
            MvccProfile::add(&self.profile.apply_ns, elapsed);
            MvccProfile::add(
                &self.profile.apply_decoded,
                u64::try_from(decoded).unwrap_or(u64::MAX),
            );
            if ferrosa_common::mem_probe::installed() {
                tracing::info!(
                    phase = "mvcc.on_postgres_apply",
                    decoded,
                    live_mib = ferrosa_common::mem_probe::live_mib(),
                    "pg commit residency"
                );
            }
        }
        Ok(())
    }
}

fn decode_row_changes(metadata: &[Vec<u8>]) -> Result<Vec<RowChange>, String> {
    let mut changes = Vec::new();
    for payload in metadata {
        let mut partition_changes = crate::row_change_codec::decode_partition(payload)
            .map_err(|error| format!("decode PostgreSQL MVCC apply metadata: {error}"))?;
        changes.append(&mut partition_changes);
    }
    Ok(changes)
}

fn row_changed(state: &State, snapshot: &MvccSnapshot, row: &RowKey) -> bool {
    state
        .local
        .get(&row.table)
        .and_then(|table| table.get(&row.key))
        .is_some_and(|v| v.seq > snapshot.read_ts)
        || snapshot.cluster_ts.is_some_and(|ts| {
            state
                .cluster
                .get(&row.table)
                .and_then(|table| table.get(&row.key))
                .and_then(|v| v.get_max())
                .is_some_and(|(latest, _)| *latest > ts)
        })
}
fn validate_snapshot(
    state: &State,
    snapshot: &MvccSnapshot,
    tables: &HashSet<String>,
) -> Result<(), MvccCommitError> {
    let active = state
        .active_snapshots
        .get(&snapshot.id)
        .ok_or(MvccCommitError::SnapshotExpired)?;
    if active
        .reads
        .iter()
        .any(|row| row_changed(state, snapshot, row))
        || tables.iter().any(|table| {
            state
                .table_epochs
                .get(table)
                .is_some_and(|epoch| *epoch > snapshot.read_ts)
        })
        || state.reservations.values().any(|other| {
            other
                .writes
                .iter()
                .any(|row| active.reads.contains(row) || tables.contains(&row.table))
        })
    {
        return Err(MvccCommitError::SerializationFailure);
    }
    Ok(())
}

/// Estimated resident bytes of a `Value`'s heap payload (the inline slot is
/// counted separately). Underestimates nothing that matters for a comparison
/// and deliberately does NOT allocate: it is a walk, not a serialization.
fn value_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Null
        | Value::Int(_)
        | Value::Bool(_)
        | Value::Float(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Inet(_) => 0,
        Value::Bytea(bytes) => bytes.len(),
        Value::Text(text) | Value::JsonPath(text) => text.len(),
        Value::Numeric { unscaled, .. } => unscaled.to_signed_bytes_le().len(),
        Value::Jsonb(doc) => doc.as_bytes().len(),
        Value::TextArray(items) => items
            .iter()
            .map(|item| item.as_ref().map_or(0, String::len))
            .sum(),
    }
}

fn values_bytes(values: &[Value]) -> usize {
    std::mem::size_of_val(values) + values.iter().map(value_heap_bytes).sum::<usize>()
}

fn row_bytes(row: &Row) -> usize {
    values_bytes(&row.0)
}

fn history_stats(state: &State) -> MvccHistoryStats {
    let mut stats = MvccHistoryStats {
        applied_accord_txns: state.applied_accord_txns.len(),
        ..Default::default()
    };
    let mut local = HashSet::new();
    let mut cluster = HashSet::new();
    for root in
        std::iter::once(&state.local).chain(state.active_snapshots.values().map(|a| &a.local))
    {
        for (table, rows) in root {
            for (key, version) in rows {
                if local.insert((table, key, Arc::as_ptr(&version.row))) {
                    stats.local_versions += 1;
                    stats.local_bytes +=
                        values_bytes(key) + version.row.as_ref().as_ref().map_or(0, row_bytes);
                }
            }
        }
    }
    for root in
        std::iter::once(&state.cluster).chain(state.active_snapshots.values().map(|a| &a.cluster))
    {
        for (table, rows) in root {
            for (key, versions) in rows {
                for (ts, row) in versions {
                    if cluster.insert((table, key, ts, Arc::as_ptr(row))) {
                        stats.distributed_versions += 1;
                        stats.distributed_bytes +=
                            values_bytes(key) + row.as_ref().as_ref().map_or(0, row_bytes);
                    }
                }
            }
        }
    }
    stats.local_keys = state.local.values().map(PersistentMap::len).sum();
    stats.distributed_keys = state.cluster.values().map(PersistentMap::len).sum();
    stats
}

// No `PartialEq`/`Eq`: `Storage` now carries `ferrosa_common::Error`, which
// wraps `std::io::Error` and is deliberately not comparable. Match on the
// variant instead of comparing whole values.
#[derive(Debug)]
pub(crate) enum MvccCommitError {
    SerializationFailure,
    SnapshotExpired,
    /// The commit's storage write failed, carrying the TYPED error.
    ///
    /// This used to be a `String`. Stringifying it here destroyed the one thing
    /// the PostgreSQL front end needs to answer the client correctly: whether
    /// the failure was deliberate backpressure (`Error::is_backpressure()`) or a
    /// real fault. Without the type every commit failure mapped to SQLSTATE
    /// `58000 system_error`, which drivers do not retry, so a producer
    /// outrunning the device was never told to slow down.
    Storage(ferrosa_common::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    // Count only allocations made by the current test thread. Other tests and
    // background runtimes must not affect this allocation regression.
    pub(super) mod allocation_probe {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
        }

        struct CountingAllocator;

        fn record() {
            let _ = COUNT.try_with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
        }

        unsafe impl GlobalAlloc for CountingAllocator {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                record();
                unsafe { System.alloc(layout) }
            }
            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                record();
                unsafe { System.alloc_zeroed(layout) }
            }
            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
                record();
                unsafe { System.realloc(ptr, layout, size) }
            }
            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                unsafe { System.dealloc(ptr, layout) }
            }
        }

        #[global_allocator]
        static ALLOCATOR: CountingAllocator = CountingAllocator;

        pub fn count(f: impl FnOnce()) -> usize {
            struct Reset;
            impl Drop for Reset {
                fn drop(&mut self) {
                    COUNT.with(|count| count.set(None));
                }
            }
            COUNT.with(|count| {
                assert!(count.get().is_none());
                count.set(Some(0));
            });
            let reset = Reset;
            f();
            let result = COUNT.with(|count| count.get().unwrap());
            drop(reset);
            result
        }
    }

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
        let overlay = manager.table_overlay(snapshot, table).unwrap();
        assert_eq!(overlay.get(key), Some(&expected));
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

    /// Behavioral liveness red: the old state mutex blocks BEGIN and its read
    /// while another commit is paused inside apply. Release the writer before
    /// asserting, so even the negative control leaves no blocked test threads.
    #[test]
    fn snapshot_reads_progress_while_an_apply_is_paused() {
        let manager = Arc::new(MvccManager::default());
        let initial = manager.snapshot();
        manager
            .commit(
                &initial,
                &HashSet::new(),
                vec![change(None, Some(row("before")))],
                || Ok(()),
            )
            .unwrap();
        drop(initial);

        let writer_snapshot = manager.snapshot();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writer_manager = manager.clone();
        let writer = std::thread::spawn(move || {
            writer_manager
                .commit(
                    &writer_snapshot,
                    &HashSet::new(),
                    vec![change(Some(row("before")), Some(row("after")))],
                    || {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok(())
                    },
                )
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let (read_tx, read_rx) = std::sync::mpsc::channel();
        let reader_manager = manager.clone();
        let reader = std::thread::spawn(move || {
            let snapshot = reader_manager.snapshot();
            let value = reader_manager
                .table_overlay(&snapshot, "public.items")
                .unwrap()
                .get(&vec![Value::Int(1)])
                .cloned()
                .flatten();
            read_tx.send(value).unwrap();
        });
        let observed = read_rx.recv_timeout(Duration::from_secs(5));
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();

        assert!(
            observed.is_ok(),
            "snapshot read waited for a paused storage apply"
        );
        assert_eq!(observed.unwrap(), Some(row("before")));
    }

    #[test]
    fn snapshot_reads_the_version_visible_at_begin() {
        let manager = Arc::new(MvccManager::default());
        let initial = manager.snapshot();
        manager
            .commit(
                &initial,
                &HashSet::new(),
                vec![change(None, Some(row("before")))],
                || Ok(()),
            )
            .unwrap();
        let reader = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(
                &writer,
                &HashSet::new(),
                vec![change(Some(row("before")), Some(row("after")))],
                || Ok(()),
            )
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
            .commit(
                &writer,
                &HashSet::new(),
                vec![change(None, Some(row("new")))],
                || Ok(()),
            )
            .unwrap();

        let result = manager.commit(
            &reader,
            &HashSet::from(["public.items".to_string()]),
            Vec::new(),
            || Ok(()),
        );
        assert!(
            matches!(result, Err(MvccCommitError::SerializationFailure)),
            "{result:?}"
        );
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
            .commit(
                &setup,
                &HashSet::new(),
                vec![change(None, Some(row("v1")))],
                || Ok(()),
            )
            .unwrap();
        drop(setup);

        let reader = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(
                &writer,
                &HashSet::new(),
                vec![change(Some(row("v1")), Some(row("v2")))],
                || Ok(()),
            )
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

    // Regression controls for the new reservation path. These complement the
    // original paused-apply behavioral red; each mutation is exercised below.
    #[test]
    fn disjoint_writer_progresses_and_overlap_is_refused_before_effects() {
        let manager = Arc::new(MvccManager::default());
        let writer_snapshot = manager.snapshot();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writer_manager = manager.clone();
        let writer = std::thread::spawn(move || {
            writer_manager.commit(
                &writer_snapshot,
                &HashSet::new(),
                vec![change(Some(row("before")), Some(row("after")))],
                || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                },
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let during = manager.snapshot();
        let view = manager.table_view(&during, "public.items");
        assert_eq!(
            view.row(&vec![Value::Int(1)]).unwrap().unwrap().as_ref(),
            &Some(row("before"))
        );
        let effects = AtomicU64::new(0);
        for kind in ["write", "point", "scan"] {
            let snapshot = manager.snapshot();
            let mut tables = HashSet::new();
            let mut changes = Vec::new();
            match kind {
                "write" => changes.push(change(Some(row("before")), Some(row("overlap")))),
                "point" => manager.record_point_read(
                    &snapshot,
                    RowKey {
                        table: "public.items".into(),
                        key: vec![Value::Int(1)],
                    },
                ),
                "scan" => {
                    tables.insert("public.items".into());
                }
                _ => unreachable!(),
            }
            let result = manager.commit(&snapshot, &tables, changes, || {
                effects.fetch_add(1, Ordering::Relaxed);
                Ok(())
            });
            assert!(
                matches!(result, Err(MvccCommitError::SerializationFailure)),
                "{kind}: {result:?}"
            );
        }
        assert_eq!(effects.load(Ordering::Relaxed), 0);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let other_manager = manager.clone();
        let other = std::thread::spawn(move || {
            let mut disjoint = change(None, Some(row("disjoint")));
            disjoint.key = vec![Value::Int(2)];
            let result = other_manager.commit(
                &other_manager.snapshot(),
                &HashSet::new(),
                vec![disjoint],
                || Ok(()),
            );
            done_tx.send(result).unwrap();
        });
        let progressed = done_rx.recv_timeout(Duration::from_secs(5));
        release_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        other.join().unwrap();
        assert!(
            progressed.is_ok(),
            "disjoint writer waited for paused apply"
        );
        progressed.unwrap().unwrap();
        let latest = manager
            .table_overlay(&manager.snapshot(), "public.items")
            .unwrap();
        assert_eq!(latest.get(&vec![Value::Int(1)]), Some(&Some(row("after"))));
        assert_eq!(
            latest.get(&vec![Value::Int(2)]),
            Some(&Some(row("disjoint")))
        );
        assert_eq!(
            view.row(&vec![Value::Int(1)]).unwrap().unwrap().as_ref(),
            &Some(row("before"))
        );
    }

    #[test]
    fn metadata_cas_interference_never_repeats_apply_and_failure_releases_reservation() {
        let manager = Arc::new(MvccManager::default());
        let calls = AtomicU64::new(0);
        let snapshot = manager.snapshot();
        manager
            .commit(
                &snapshot,
                &HashSet::new(),
                vec![change(Some(row("before")), Some(row("after")))],
                || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    // Additional root replacement after reservation.
                    for _ in 0..32 {
                        let concurrent = manager.snapshot();
                        manager.record_point_read(
                            &concurrent,
                            RowKey {
                                table: "other".into(),
                                key: vec![Value::Int(5)],
                            },
                        );
                    }
                    // Force an actual failed metadata CAS in final publication, not
                    // merely a root replacement somewhere before that CAS loads.
                    let interfering = manager.clone();
                    BEFORE_CAS.with(|slot| {
                        *slot.borrow_mut() = Some(Box::new(move || {
                            let concurrent = interfering.snapshot();
                            drop(concurrent);
                        }))
                    });
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(CAS_RETRIES.with(|retries| retries.get()) >= 1);
        let snapshot = manager.snapshot();
        let result = manager.commit(
            &snapshot,
            &HashSet::new(),
            vec![change(Some(row("after")), Some(row("failed")))],
            || {
                Err(ferrosa_common::Error::InvalidData(
                    "injected storage failure".into(),
                ))
            },
        );
        assert!(matches!(result, Err(MvccCommitError::Storage(_))));
        assert!(manager.state.load().reservations.is_empty());
        assert_eq!(
            manager
                .table_overlay(&manager.snapshot(), "public.items")
                .unwrap()
                .get(&vec![Value::Int(1)]),
            Some(&Some(row("after")))
        );
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = manager.commit(
                &snapshot,
                &HashSet::new(),
                vec![change(Some(row("after")), None)],
                || panic!("injected apply unwind"),
            );
        }));
        assert!(unwound.is_err());
        assert!(manager.state.load().reservations.is_empty());
        manager
            .commit(
                &snapshot,
                &HashSet::new(),
                vec![change(Some(row("after")), Some(row("retry")))],
                || Ok(()),
            )
            .unwrap();
        assert_eq!(
            manager
                .table_overlay(&manager.snapshot(), "public.items")
                .unwrap()
                .get(&vec![Value::Int(1)]),
            Some(&Some(row("retry")))
        );
    }

    /// A failed first touch must not manufacture a write epoch from unrelated
    /// commits. The old reader's key has never successfully changed.
    #[test]
    fn failed_first_touch_does_not_conflict_with_an_unchanged_old_point_read() {
        let manager = MvccManager::default();
        let old = manager.snapshot();
        manager.record_point_read(
            &old,
            RowKey {
                table: "public.items".into(),
                key: vec![Value::Int(1)],
            },
        );
        let mut unrelated = change(None, Some(row("other")));
        unrelated.key = vec![Value::Int(2)];
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![unrelated],
                || Ok(()),
            )
            .unwrap();
        let failed = manager.commit(
            &manager.snapshot(),
            &HashSet::new(),
            vec![change(Some(row("before")), Some(row("failed")))],
            || {
                Err(ferrosa_common::Error::InvalidData(
                    "injected storage failure".into(),
                ))
            },
        );
        assert!(matches!(failed, Err(MvccCommitError::Storage(_))));
        manager
            .validate_commit(&old, &HashSet::new())
            .expect("failed first touch must not create a committed version");
        assert_eq!(
            manager
                .table_overlay(&old, "public.items")
                .unwrap()
                .get(&vec![Value::Int(1)]),
            Some(&Some(row("before")))
        );
    }

    #[test]
    fn genuinely_racing_overlapping_writers_publish_exactly_one_value() {
        let manager = Arc::new(MvccManager::default());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let effects = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = ["left", "right"]
            .into_iter()
            .map(|value| {
                let snapshot = manager.snapshot();
                let manager = manager.clone();
                let barrier = barrier.clone();
                let effects = effects.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    (
                        value,
                        manager.commit(
                            &snapshot,
                            &HashSet::new(),
                            vec![change(None, Some(row(value)))],
                            || {
                                effects.fetch_add(1, Ordering::Relaxed);
                                Ok(())
                            },
                        ),
                    )
                })
            })
            .collect();
        barrier.wait();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(effects.load(Ordering::Relaxed), 1);
        assert_eq!(
            outcomes.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        for (value, result) in outcomes {
            match result {
                Ok(_) => assert_eq!(
                    manager
                        .table_overlay(&manager.snapshot(), "public.items")
                        .unwrap()
                        .get(&vec![Value::Int(1)]),
                    Some(&Some(row(value)))
                ),
                Err(MvccCommitError::SerializationFailure) => {}
                Err(error) => panic!("unexpected refusal: {error:?}"),
            }
        }
    }

    #[test]
    fn snapshot_lease_clones_and_expiry_release_actual_payload_owners() {
        let manager = MvccManager::default();
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![change(None, Some(row("v1")))],
                || Ok(()),
            )
            .unwrap();
        let old = manager.snapshot();
        let cloned = old.clone();
        let shared = manager
            .table_view(&old, "public.items")
            .row(&vec![Value::Int(1)])
            .unwrap()
            .unwrap();
        let weak = Arc::downgrade(&shared);
        drop(shared);
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![change(Some(row("v1")), None)],
                || Ok(()),
            )
            .unwrap();
        assert!(weak.upgrade().is_some());
        drop(old);
        assert!(weak.upgrade().is_some());
        assert_eq!(
            manager
                .table_overlay(&cloned, "public.items")
                .unwrap()
                .get(&vec![Value::Int(1)]),
            Some(&Some(row("v1")))
        );
        drop(cloned);
        assert!(
            weak.upgrade().is_none(),
            "last lease must free historical payload"
        );
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![change(None, Some(row("v2")))],
                || Ok(()),
            )
            .unwrap();
        let expired = manager.snapshot();
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![change(Some(row("v2")), Some(row("v3")))],
                || Ok(()),
            )
            .unwrap();
        let view = manager.table_view(&expired, "public.items");
        let shared = view.row(&vec![Value::Int(1)]).unwrap().unwrap();
        let weak = Arc::downgrade(&shared);
        drop(shared);
        assert!(weak.upgrade().is_some());
        let mut held_iterator = view.rows().unwrap();
        manager.expire_all_snapshots_for_test();
        assert!(
            weak.upgrade().is_none(),
            "expired handles must not pin roots"
        );
        assert!(matches!(
            held_iterator.next().unwrap(),
            Err(MvccCommitError::SnapshotExpired)
        ));
        drop(held_iterator);
        assert!(matches!(
            view.row(&vec![Value::Int(1)]),
            Err(MvccCommitError::SnapshotExpired)
        ));
    }

    #[test]
    fn cluster_late_prepare_out_of_order_apply_and_replay_preserve_timestamp_views() {
        use ferrosa_storage::accord::PostgresMvccApplyObserver;
        let manager = MvccManager::default();
        let early = manager.snapshot_with_cluster_ts(Timestamp::synthetic(15));
        let middle = manager.snapshot_with_cluster_ts(Timestamp::synthetic(25));
        let latest = manager.snapshot_with_cluster_ts(Timestamp::synthetic(35));
        let encode = |before, after| {
            crate::row_change_codec::encode_partition(&[change(
                Some(row(before)),
                Some(row(after)),
            )])
            .unwrap()
        };
        let t30 = Timestamp::synthetic(30);
        let id30 = TxnId::new(1, t30);
        manager
            .prepare_postgres_apply(id30, t30, &[encode("v0", "v30")])
            .unwrap();
        let t20 = Timestamp::synthetic(20);
        let id20 = TxnId::new(1, t20);
        // Skipping prepare must still preserve metadata, and must not overwrite
        // a newer physical/apply head when callbacks arrive out of order.
        manager
            .on_postgres_apply(id30, t30, &[encode("v0", "v30")])
            .unwrap();
        manager
            .on_postgres_apply(id20, t20, &[encode("v0", "v20")])
            .unwrap();
        for (snapshot, expected) in [(&early, "v0"), (&middle, "v20"), (&latest, "v30")] {
            assert_eq!(
                manager
                    .table_overlay(snapshot, "public.items")
                    .unwrap()
                    .get(&vec![Value::Int(1)]),
                Some(&Some(row(expected)))
            );
        }
        let seq = manager.current_commit_seq();
        manager
            .prepare_postgres_apply(id20, t20, &[encode("v0", "v20")])
            .unwrap();
        manager
            .on_postgres_apply(id20, t20, &[encode("v0", "v20")])
            .unwrap();
        assert_eq!(manager.current_commit_seq(), seq);
        assert_eq!(
            manager
                .table_overlay(
                    &manager.snapshot_with_cluster_ts(Timestamp::synthetic(40)),
                    "public.items"
                )
                .unwrap()
                .get(&vec![Value::Int(1)]),
            Some(&Some(row("v30")))
        );
        drop(early);
        drop(middle);
        drop(latest);
        assert_eq!(
            manager.history_stats().distributed_versions,
            1,
            "replay must not repopulate obsolete head history"
        );
    }

    /// A timestamp older than the retained head cannot fall through to latest
    /// physical storage. Reject unavailable history rather than leaking it.
    #[test]
    fn unavailable_cluster_history_fails_instead_of_reading_latest_storage() {
        use ferrosa_storage::accord::PostgresMvccApplyObserver;
        let manager = MvccManager::default();
        let ts = Timestamp::synthetic(30);
        let metadata = crate::row_change_codec::encode_partition(&[change(
            Some(row("old")),
            Some(row("latest")),
        )])
        .unwrap();
        manager
            .on_postgres_apply(TxnId::new(1, ts), ts, &[metadata])
            .unwrap();
        let stale = manager.snapshot_with_cluster_ts(Timestamp::synthetic(25));
        let view = manager.table_view(&stale, "public.items");
        assert!(
            matches!(
                view.row(&vec![Value::Int(1)]),
                Err(MvccCommitError::SnapshotExpired)
            ),
            "missing history must not use the current physical row"
        );
        assert!(matches!(
            view.rows().unwrap().next().unwrap(),
            Err(MvccCommitError::SnapshotExpired)
        ));
    }

    #[test]
    fn per_key_commit_work_does_not_copy_unrelated_row_history() {
        let mut curve = Vec::new();
        for size in [128, 1024, 8192] {
            let manager = MvccManager::default();
            let changes = (0..size)
                .map(|i| {
                    let mut item = change(None, Some(row("resident")));
                    item.key = vec![Value::Int(i)];
                    item
                })
                .collect();
            manager
                .commit(&manager.snapshot(), &HashSet::new(), changes, || Ok(()))
                .unwrap();
            let snapshot = manager.snapshot();
            let allocations = allocation_probe::count(|| {
                manager
                    .commit(
                        &snapshot,
                        &HashSet::new(),
                        vec![change(Some(row("resident")), Some(row("changed")))],
                        || Ok(()),
                    )
                    .unwrap();
            });
            curve.push((size, allocations));
            assert_eq!(
                manager
                    .table_overlay(&manager.snapshot(), "public.items")
                    .unwrap()
                    .get(&vec![Value::Int(size - 1)]),
                Some(&Some(row("resident")))
            );
        }
        if let Ok(path) = env::var("FERROSA_MVCC_SCALE_EVIDENCE") {
            std::fs::write(path, format!("{curve:?}\n")).unwrap();
        }
        // Measured on the production entry point: 42 / 45 / 45 allocations at
        // 128 / 1024 / 8192 resident keys. A 64x resident-size increase can add
        // HAMT depth, never a copy per row. 4x allows randomized HAMT shape
        // variation while rejecting whole-root copying (64x). No timing gate.
        assert!(curve[2].1 < curve[0].1 * 4, "allocation curve {curve:?}");
    }
    #[test]
    fn mvcc_config_defaults_scan_buffer_and_snapshot_max_age() {
        let config = MvccConfig::from_overrides(None, None, None).unwrap();
        assert_eq!(config.scan_buffer_rows, DEFAULT_SCAN_BUFFER_ROWS);
        assert_eq!(config.max_snapshot_age, Duration::from_secs(600));
    }

    #[test]
    fn mvcc_config_accepts_scan_buffer_and_snapshot_max_age_overrides() {
        let config = MvccConfig::from_overrides(Some("128"), Some("30000"), Some("250")).unwrap();
        assert_eq!(config.scan_buffer_rows, 128);
        assert_eq!(config.max_snapshot_age, Duration::from_secs(30));
        assert_eq!(config.snapshot_reaper_interval, Duration::from_millis(250));
        assert_eq!(MvccManager::with_config(config).scan_buffer_rows(), 128);
    }

    #[test]
    fn mvcc_config_rejects_zero_and_malformed_overrides() {
        assert!(MvccConfig::from_overrides(Some("0"), None, None).is_err());
        assert!(MvccConfig::from_overrides(None, Some("0"), None).is_err());
        assert!(MvccConfig::from_overrides(None, None, Some("0")).is_err());
        assert!(MvccConfig::from_overrides(Some("x"), None, None).is_err());
        assert!(MvccConfig::from_overrides(None, Some("x"), None).is_err());
    }

    /// RED-first (caps-gone-postgres): the retired `FERROSA_POSTGRES_MAX_TXN_WRITES`
    /// knob bounds NOTHING any more — the front end's write-set spills — but
    /// `from_env` still PARSES it, and a zero or malformed value makes the WHOLE
    /// MVCC config fall back to its defaults. A typo in a dead knob therefore
    /// silently resets the live `scan_buffer_rows` setting. Removing the knob
    /// removes that coupling.
    ///
    /// Crosses the real config-resolution path the server runs at startup
    /// (`ferrosa::main` calls `MvccManager::from_env()` -> `MvccConfig::from_env()`),
    /// not a unit shim: the assertion is on the resolution `from_env` performs.
    #[test]
    fn a_retired_write_cap_setting_cannot_reset_the_other_mvcc_settings() {
        static ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_GUARD
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        const VARS: [&str; 4] = [
            "FERROSA_POSTGRES_MAX_TXN_WRITES",
            "FERROSA_POSTGRES_SCAN_BUFFER_ROWS",
            "FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS",
            "FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS",
        ];
        let saved: Vec<(String, Option<String>)> = VARS
            .iter()
            .map(|name| ((*name).to_string(), env::var(name).ok()))
            .collect();

        // A retired, value-less knob set to a bad value, beside a LIVE live knob.
        env::set_var("FERROSA_POSTGRES_MAX_TXN_WRITES", "0");
        env::set_var("FERROSA_POSTGRES_SCAN_BUFFER_ROWS", "128");

        let config = MvccConfig::from_env();

        for (name, value) in saved {
            match value {
                Some(value) => env::set_var(&name, value),
                None => env::remove_var(&name),
            }
        }

        assert_eq!(
            config.scan_buffer_rows, 128,
            "a bad value for the RETIRED write-count knob must not reset the live \
             scan-buffer setting to its default"
        );
    }

    #[test]
    fn expiring_an_old_snapshot_releases_history_and_rejects_commit() {
        let config = MvccConfig::from_overrides(None, Some("5"), None).unwrap();
        let manager = MvccManager::with_config(config);
        let setup = manager.snapshot();
        manager
            .commit(
                &setup,
                &HashSet::new(),
                vec![change(None, Some(row("v1")))],
                || Ok(()),
            )
            .unwrap();
        drop(setup);

        let old_snapshot = manager.snapshot();
        let writer = manager.snapshot();
        manager
            .commit(
                &writer,
                &HashSet::new(),
                vec![change(Some(row("v1")), Some(row("v2")))],
                || Ok(()),
            )
            .unwrap();
        drop(writer);
        assert_eq!(manager.retained_version_count(), 2);

        let expired = manager.expire_snapshots_before(Instant::now() + Duration::from_millis(6));
        assert_eq!(expired, 1);
        assert_eq!(manager.active_snapshot_count(), 0);
        assert_eq!(manager.retained_version_count(), 1);
        assert!(
            matches!(
                manager.validate_commit(&old_snapshot, &HashSet::new()),
                Err(MvccCommitError::SnapshotExpired)
            ),
            "an expired snapshot must refuse its commit"
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
        let metadata = crate::row_change_codec::encode_partition(&before).unwrap();
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
        let old_rows = manager
            .table_overlay(&old_snapshot, "public.items")
            .unwrap();
        let committed_rows = manager
            .table_overlay(&committed_snapshot, "public.items")
            .unwrap();

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
