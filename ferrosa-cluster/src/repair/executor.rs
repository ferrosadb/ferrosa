//! [`SessionExecutor`] implementations.
//!
//! The repair algorithm itself is in [`crate::repair`]; this module wires it
//! to actual data stores. The production wiring goes through internode RPC
//! (see follow-up PRs). This module gives a [`LocalRepairExecutor`] that runs
//! the algorithm between two [`RepairStore`]s in the same process — used by
//! tests to validate convergence end-to-end against real
//! [`ferrosa_storage::engine::StorageEngine`]s without standing up the wire
//! protocol.

use async_trait::async_trait;
use std::sync::Arc;

use ferrosa_common::DecoratedKey;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use super::coordinator::{SessionExecutor, SessionStats};
use super::{compute_repair_plan, RepairPlan};

/// Abstraction over a node's data store, scoped to the operations repair
/// needs: read partitions in a token range, and apply partitions received
/// from a peer. Implemented for `Arc<StorageEngine>` (production) and for
/// `Arc<Mutex<Vec<Partition>>>` (tests).
#[async_trait]
pub trait RepairStore: Send + Sync {
    /// Return all partitions for `table` whose tokens fall in
    /// `[range_start, range_end)`. Order is unspecified; callers index by
    /// partition key.
    async fn read_range(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<Vec<Partition>, String>;

    /// Apply the given partitions, last-write-wins on a per-cell basis.
    /// Used to land partitions streamed from a peer during repair.
    async fn apply_partitions(
        &self,
        table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String>;
}

#[async_trait]
impl RepairStore for Arc<StorageEngine> {
    async fn read_range(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<Vec<Partition>, String> {
        // StorageEngine::read_range is sync + does file I/O; offload so we
        // don't block the async worker on a potentially-large scan.
        let engine = self.clone();
        let table = table.clone();
        let result: Result<Vec<Partition>, String> = tokio::task::spawn_blocking(move || {
            let all = StorageEngine::read_range(&engine, &table, None, None, usize::MAX)
                .map_err(|e| format!("read_range: {e}"))?;
            Ok(all
                .into_iter()
                .filter(|p| p.key.token.0 >= range_start && p.key.token.0 < range_end)
                .collect())
        })
        .await
        .map_err(|e| format!("read_range join: {e}"))?;
        result
    }

    async fn apply_partitions(
        &self,
        table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String> {
        let engine = self.clone();
        let table = table.clone();
        let parts = partitions.to_vec();
        tokio::task::spawn_blocking(move || {
            for partition in parts {
                for row in partition.rows.iter() {
                    let ts = row
                        .cells
                        .iter()
                        .map(|(_, c)| c.timestamp)
                        .max()
                        .unwrap_or(row.primary_key_liveness.timestamp);
                    engine
                        .write(&table, &partition.key, row.clone(), ts)
                        .map_err(|e| format!("apply write: {e}"))?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| format!("apply_partitions join: {e}"))?
    }
}

/// In-process executor: runs anti-entropy between two stores that both live
/// locally. Used by tests + by single-node simulations. The production wire
/// executor (RPC-backed) has the same shape but `remote` is reached via the
/// internode protocol.
pub struct LocalRepairExecutor<L: RepairStore + 'static, R: RepairStore + 'static> {
    pub local: Arc<L>,
    /// `peer_id -> remote store` map. The coordinator dispatches sessions
    /// against `peer_id`; this map looks up the in-process store to use.
    pub remotes: std::collections::HashMap<u64, Arc<R>>,
}

#[async_trait]
impl<L: RepairStore + 'static, R: RepairStore + 'static> SessionExecutor
    for LocalRepairExecutor<L, R>
{
    async fn run_session(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
        peer: u64,
    ) -> Result<SessionStats, String> {
        let remote = self
            .remotes
            .get(&peer)
            .ok_or_else(|| format!("unknown peer {peer}"))?;

        let local_parts = self.local.read_range(table, range_start, range_end).await?;
        let remote_parts = remote.read_range(table, range_start, range_end).await?;

        let plan: RepairPlan =
            compute_repair_plan(&local_parts, &remote_parts, range_start, range_end);

        let streamed_out = plan.a_to_b.len() as u64;
        let streamed_in = plan.b_to_a.len() as u64;
        let ties = plan.timestamp_ties.len() as u64;

        // Push local-newer copies to the remote.
        if !plan.a_to_b.is_empty() {
            remote.apply_partitions(table, &plan.a_to_b).await?;
        }
        // Pull remote-newer copies into local.
        if !plan.b_to_a.is_empty() {
            self.local.apply_partitions(table, &plan.b_to_a).await?;
        }

        Ok(SessionStats {
            partitions_streamed_in: streamed_in,
            partitions_streamed_out: streamed_out,
            timestamp_ties: ties,
        })
    }
}

// ───────────── In-memory RepairStore for tests ─────────────

/// In-memory `RepairStore` for unit tests.
///
/// Stores partitions in a `Vec`. `apply_partitions` deduplicates by partition
/// key — keeping the partition whose newest timestamp is highest. This is the
/// minimum needed to make convergence assertions work; a real engine has
/// richer semantics (per-cell LWW, deletion masks, etc.) handled by
/// `StorageEngine::write`.
pub struct InMemoryRepairStore {
    inner: tokio::sync::Mutex<Vec<(DecoratedKey, Partition)>>,
}

impl InMemoryRepairStore {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn insert(&self, p: Partition) {
        self.apply_one(p).await;
    }

    async fn apply_one(&self, p: Partition) {
        let mut store = self.inner.lock().await;
        if let Some(slot) = store.iter_mut().find(|(k, _)| k == &p.key) {
            if super::newest_partition_timestamp(&p) >= super::newest_partition_timestamp(&slot.1) {
                slot.1 = p;
            }
        } else {
            store.push((p.key.clone(), p));
        }
    }

    /// Snapshot the current contents — used in test assertions.
    pub async fn snapshot(&self) -> Vec<Partition> {
        let store = self.inner.lock().await;
        store.iter().map(|(_, p)| p.clone()).collect()
    }
}

impl Default for InMemoryRepairStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RepairStore for InMemoryRepairStore {
    async fn read_range(
        &self,
        _table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<Vec<Partition>, String> {
        let store = self.inner.lock().await;
        Ok(store
            .iter()
            .filter(|(k, _)| k.token.0 >= range_start && k.token.0 < range_end)
            .map(|(_, p)| p.clone())
            .collect())
    }

    async fn apply_partitions(
        &self,
        _table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String> {
        for p in partitions {
            self.apply_one(p.clone()).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_partition_at(token: i64, key_bytes: &[u8], value: &[u8], ts: i64) -> Partition {
        let key = PartitionKey::new(key_bytes.to_vec());
        let dk = DecoratedKey {
            token: Token(token),
            key,
        };
        let cell = CellValue::live(value.to_vec(), ts);
        let row = Row {
            clustering: vec![],
            cells: vec![(0, cell)],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        }
    }

    fn keys_in(parts: &[Partition]) -> std::collections::BTreeSet<Vec<u8>> {
        parts
            .iter()
            .map(|p| p.key.key.as_bytes().to_vec())
            .collect()
    }

    fn value_for_key<'a>(parts: &'a [Partition], key: &[u8]) -> Option<&'a [u8]> {
        parts
            .iter()
            .find(|p| p.key.key.as_bytes() == key)
            .and_then(|p| p.rows.first())
            .and_then(|r| r.cells.first())
            .and_then(|(_, c)| c.value.as_deref())
    }

    #[tokio::test]
    async fn local_executor_converges_two_stores_after_one_session() {
        // Stage divergent state across two in-memory stores. Tokens are
        // picked far apart so they land in different Merkle leaves at
        // TREE_DEPTH=15 across the full i64 range.
        let a = Arc::new(InMemoryRepairStore::new());
        let b = Arc::new(InMemoryRepairStore::new());

        // k1: only on A
        a.insert(test_partition_at(100_000_000, b"k1", b"a1", 100))
            .await;
        // k2: same on both
        a.insert(test_partition_at(200_000_000, b"k2", b"same", 200))
            .await;
        b.insert(test_partition_at(200_000_000, b"k2", b"same", 200))
            .await;
        // k3: divergent — A is newer
        a.insert(test_partition_at(300_000_000, b"k3", b"new", 500))
            .await;
        b.insert(test_partition_at(300_000_000, b"k3", b"old", 100))
            .await;
        // k4: only on B
        b.insert(test_partition_at(400_000_000, b"k4", b"b4", 100))
            .await;

        let executor = LocalRepairExecutor::<InMemoryRepairStore, InMemoryRepairStore> {
            local: a.clone(),
            remotes: [(7u64, b.clone())].into_iter().collect(),
        };

        let table = TableId::new("ks", "tbl");
        let stats = executor
            .run_session(&table, i64::MIN, i64::MAX, 7)
            .await
            .unwrap();
        assert_eq!(
            stats.partitions_streamed_out, 2,
            "A→B should be k1 + k3 (newer)"
        );
        assert_eq!(stats.partitions_streamed_in, 1, "B→A should be k4");
        assert_eq!(stats.timestamp_ties, 0);

        // Both sides must converge to the same set of keys.
        let a_snap = a.snapshot().await;
        let b_snap = b.snapshot().await;
        let want_keys: std::collections::BTreeSet<Vec<u8>> = [
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ]
        .into_iter()
        .collect();
        assert_eq!(keys_in(&a_snap), want_keys);
        assert_eq!(keys_in(&b_snap), want_keys);

        // For k3, both sides must hold the A-newer ("new") value.
        assert_eq!(value_for_key(&a_snap, b"k3"), Some(&b"new"[..]));
        assert_eq!(value_for_key(&b_snap, b"k3"), Some(&b"new"[..]));
    }

    #[tokio::test]
    async fn local_executor_is_idempotent_after_convergence() {
        let a = Arc::new(InMemoryRepairStore::new());
        let b = Arc::new(InMemoryRepairStore::new());
        for i in 0..5u8 {
            let p = test_partition_at(100_000_000 * (i as i64 + 1), &[b'k', i], b"same", 1_000);
            a.insert(p.clone()).await;
            b.insert(p).await;
        }

        let executor = LocalRepairExecutor::<InMemoryRepairStore, InMemoryRepairStore> {
            local: a.clone(),
            remotes: [(7u64, b.clone())].into_iter().collect(),
        };

        let table = TableId::new("ks", "tbl");
        let stats1 = executor
            .run_session(&table, i64::MIN, i64::MAX, 7)
            .await
            .unwrap();
        let stats2 = executor
            .run_session(&table, i64::MIN, i64::MAX, 7)
            .await
            .unwrap();
        for s in [&stats1, &stats2] {
            assert_eq!(s.partitions_streamed_in, 0);
            assert_eq!(s.partitions_streamed_out, 0);
            assert_eq!(s.timestamp_ties, 0);
        }
    }

    #[tokio::test]
    async fn local_executor_records_timestamp_ties_without_streaming() {
        let a = Arc::new(InMemoryRepairStore::new());
        let b = Arc::new(InMemoryRepairStore::new());
        a.insert(test_partition_at(100_000_000, b"k", b"a-value", 500))
            .await;
        b.insert(test_partition_at(100_000_000, b"k", b"b-value", 500))
            .await;

        let executor = LocalRepairExecutor::<InMemoryRepairStore, InMemoryRepairStore> {
            local: a.clone(),
            remotes: [(7u64, b.clone())].into_iter().collect(),
        };

        let stats = executor
            .run_session(&TableId::new("ks", "tbl"), i64::MIN, i64::MAX, 7)
            .await
            .unwrap();
        assert_eq!(stats.timestamp_ties, 1);
        assert_eq!(stats.partitions_streamed_out, 0);
        assert_eq!(stats.partitions_streamed_in, 0);

        // Both sides keep their original (now-known-divergent) values.
        let a_val = value_for_key(&a.snapshot().await, b"k").unwrap().to_vec();
        let b_val = value_for_key(&b.snapshot().await, b"k").unwrap().to_vec();
        assert_eq!(a_val, b"a-value");
        assert_eq!(b_val, b"b-value");
    }
}
