//! PostgreSQL-only MVCC snapshots and optimistic serializability validation.
//!
//! CQL transactions are coordinated by Accord. This manager is deliberately
//! separate: it versions PostgreSQL row images and tracks the oldest live
//! PostgreSQL snapshot.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_sql::{Row, Value};
use ferrosa_storage::commitlog::Mutation;

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
#[derive(Default)]
pub struct MvccManager {
    state: Arc<Mutex<State>>,
    commit_gate: Arc<tokio::sync::Mutex<()>>,
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
        if let Some(cluster_ts) = snapshot.cluster_ts {
            return state
                .distributed_versions
                .iter()
                .filter(|(key, _)| key.table == table)
                .filter_map(|(key, versions)| {
                    versions
                        .range(..=cluster_ts)
                        .next_back()
                        .map(|(_, row)| (key.key.clone(), row.clone()))
                })
                .collect();
        }
        state
            .versions
            .iter()
            .filter(|(key, _)| key.table == table)
            .filter_map(|(key, versions)| {
                versions
                    .range(..=snapshot.read_ts)
                    .next_back()
                    .map(|(_, row)| (key.key.clone(), row.clone()))
            })
            .collect()
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
}
