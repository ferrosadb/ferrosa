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
use ferrosa_net::task_pool::TaskPool;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use super::coordinator::{SessionExecutor, SessionStats};

/// Abstraction over a node's data store, scoped to the operations repair
/// needs: read partitions in a token range, and apply partitions received
/// from a peer. Implemented for `Arc<StorageEngine>` (production) and for
/// `Arc<Mutex<Vec<Partition>>>` (tests).
/// Maximum number of partitions a single Fetch RPC / single
/// `read_range_chunked` call carries. Caps the per-chunk working
/// set the executor holds in flight: at this limit the multi-
/// chunk loop in `run_session` keeps memory bounded to
/// `≈ 2 × REPAIR_FETCH_CHUNK_PARTITIONS × max_partition_size`
/// regardless of how big the diff is.
pub const REPAIR_FETCH_CHUNK_PARTITIONS: usize = 64;

/// Maximum number of partitions sent in a single Apply RPC body.
/// On the sender side the executor splits the diff into batches
/// of this size before calling `apply_partitions`, so the wire
/// payload + the peer's in-flight applied state are bounded.
pub const REPAIR_APPLY_CHUNK_PARTITIONS: usize = 64;

#[async_trait]
pub trait RepairStore: Send + Sync {
    /// Return all partitions for `table` whose tokens fall in
    /// `[range_start, range_end)`. Order is unspecified; callers index by
    /// partition key.
    ///
    /// Production code should prefer [`Self::read_range_chunked`] in the
    /// executor's hot path so each step's working set stays
    /// bounded; this one-shot variant is retained for tests and
    /// callers that genuinely want the entire range materialised.
    async fn read_range(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<Vec<Partition>, String>;

    /// Chunked fetch: return at most `limit` partitions starting
    /// from `cursor.unwrap_or(range_start)`, in token order,
    /// alongside the cursor the caller should pass on the next
    /// call (or `None` when the range is exhausted).
    ///
    /// Default impl falls through to the one-shot `read_range` —
    /// fine for in-memory test stores. Production impls
    /// (`StorageEngineRepairStore`, `RemoteRepairStore`) override
    /// with a cursor-aware path that honors `limit` on the
    /// storage / wire side so peak in-flight is bounded.
    async fn read_range_chunked(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
        cursor: Option<i64>,
        limit: usize,
    ) -> Result<(Vec<Partition>, Option<i64>), String> {
        // Default: one-shot read, slice by cursor + limit.
        let all = self.read_range(table, range_start, range_end).await?;
        let mut filtered: Vec<Partition> = all
            .into_iter()
            .filter(|p| match cursor {
                Some(c) => p.key.token.0 >= c,
                None => true,
            })
            .collect();
        filtered.sort_by_key(|p| p.key.token.0);
        let next_cursor = if filtered.len() > limit {
            let next = filtered[limit].key.token.0;
            filtered.truncate(limit);
            Some(next)
        } else {
            None
        };
        Ok((filtered, next_cursor))
    }

    /// Apply the given partitions, last-write-wins on a per-cell basis.
    /// Used to land partitions streamed from a peer during repair.
    async fn apply_partitions(
        &self,
        table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String>;

    /// Build a Merkle tree summarising this store's content for the
    /// token range `[range_start, range_end)`. The Merkle leaf
    /// hashes are an order-independent XOR of per-partition
    /// content hashes — see `repair::partition_merkle_hash`.
    ///
    /// The executor uses Merkle exchange to identify the divergent
    /// leaf ranges between two replicas BEFORE fetching any
    /// partitions, so the per-session working set scales with
    /// *divergence size*, not table size — a fully-converged 1 GB
    /// table costs only two tree builds (streaming, bounded
    /// memory) plus one comparison, with zero partition transfers.
    async fn build_merkle(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<super::merkle::MerkleTree, String>;
}

/// `RepairStore` over a local [`StorageEngine`]. Used by the production
/// `LocalRepairExecutor` as the `local` side and (theoretically) by
/// a single-node repair simulation. Wraps the engine in a newtype so
/// `Arc<StorageEngineRepairStore>` cleanly unsizes to `Arc<dyn RepairStore>`
/// (the trait impl would otherwise need to be on `Arc<StorageEngine>`,
/// which then can't be wrapped in another `Arc<dyn ...>`).
pub struct StorageEngineRepairStore {
    engine: Arc<StorageEngine>,
}

impl StorageEngineRepairStore {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl RepairStore for StorageEngineRepairStore {
    async fn read_range(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<Vec<Partition>, String> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let (mut chunk, next) = self
                .read_range_chunked(
                    table,
                    range_start,
                    range_end,
                    cursor,
                    REPAIR_FETCH_CHUNK_PARTITIONS,
                )
                .await?;
            out.append(&mut chunk);
            let Some(next_cursor) = next else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(out)
    }

    async fn read_range_chunked(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
        cursor: Option<i64>,
        limit: usize,
    ) -> Result<(Vec<Partition>, Option<i64>), String> {
        // Chunked local read: only pull `limit` partitions per
        // call, probe for one extra to detect "more remaining"
        // without an extra round trip. This is the local-side
        // analogue of RepairFetchHandler's chunking on the wire —
        // matters when the executor walks a span on the *local*
        // side: a wide span on a 1 GB replica can't fit through
        // the 2 GiB cgroup if it materialises every partition in
        // one shot, even if every partition is small.
        let engine = self.engine.clone();
        let table = table.clone();
        let chunk_start = cursor.unwrap_or(range_start);
        let probe = limit.saturating_add(1);
        let result: Result<Vec<Partition>, String> = TaskPool::current("repair-read")
            .spawn_blocking(move || {
                StorageEngine::read_token_range(&engine, &table, chunk_start, range_end, probe)
                    .map_err(|e| format!("read_token_range: {e}"))
            })
            .await
            .map_err(|e| format!("read_range_chunked join: {e}"))?;
        let mut got = result?;
        got.sort_by_key(|p| p.key.token.0);
        let next_cursor = if got.len() > limit {
            let n = got[limit].key.token.0;
            got.truncate(limit);
            Some(n)
        } else {
            None
        };
        Ok((got, next_cursor))
    }

    async fn apply_partitions(
        &self,
        table: &TableId,
        partitions: &[Partition],
    ) -> Result<(), String> {
        let engine = self.engine.clone();
        let table = table.clone();
        let parts = partitions.to_vec();
        TaskPool::current("repair-apply")
            .spawn_blocking(move || {
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

    async fn build_merkle(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<super::merkle::MerkleTree, String> {
        // build_tree_for_range is sync and walks the local SSTables
        // page-by-page via read_token_range — offload so we don't
        // block the async worker on a multi-GB scan. Acquire the
        // process-wide REPAIR_BUILD_SEMAPHORE first so initiator
        // and RPC-handler builds share one budget; without it,
        // an N-node cluster runs ~N concurrent full-table walks
        // per repair on the largest replica.
        let _permit = super::REPAIR_BUILD_SEMAPHORE
            .acquire()
            .await
            .map_err(|e| format!("build_merkle semaphore: {e}"))?;
        let engine = self.engine.clone();
        let table = table.clone();
        TaskPool::current("repair-merkle")
            .spawn_blocking(move || {
                super::build_tree_for_range(&engine, &table, range_start, range_end)
                    .map_err(|e| format!("build_tree_for_range: {e}"))
            })
            .await
            .map_err(|e| format!("build_merkle join: {e}"))?
    }
}

/// Executor that drives anti-entropy between a local `RepairStore` and one
/// or more remote `RepairStore`s. The "remote" side is type-erased via a
/// trait object so production can plug in [`super::rpc::RemoteRepairStore`]
/// (RPC) and tests can plug in [`InMemoryRepairStore`] without recompiling.
///
/// `local` and `remotes` use the same trait — the local side is just
/// "remote 0". In practice the local store is an `Arc<StorageEngine>`
/// (the same engine the coordinator runs on) and each remote is an
/// `Arc<RemoteRepairStore>` pointed at one peer.
pub struct LocalRepairExecutor {
    pub local: Arc<dyn RepairStore>,
    /// `peer_id -> remote store`. The coordinator dispatches sessions
    /// against `peer_id`; this map looks up the store to use.
    pub remotes: std::collections::HashMap<u64, Arc<dyn RepairStore>>,
}

#[async_trait]
impl SessionExecutor for LocalRepairExecutor {
    /// Repair `[range_start, range_end)` between local and `peer` via
    /// Merkle-then-stream:
    ///
    /// 1. Both sides build a Merkle tree by streaming their local
    ///    replica through `build_merkle` (bounded memory, see
    ///    `MERKLE_BUILD_BATCH`).
    /// 2. Compare leaf hashes via `divergent_leaf_ranges`.
    /// 3. For each maximal-contiguous run of divergent leaves, fetch
    ///    only those partitions from both sides, diff, and apply
    ///    in both directions.
    ///
    /// When the replicas already agree the Merkle exchange short-
    /// circuits to "no divergent leaves" and zero partition data
    /// crosses the wire — the structural difference from the prior
    /// "fetch-all-and-diff" shape, which paid O(table_size) per
    /// session regardless of divergence.
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

        // Phase 1 — Merkle exchange.
        let (local_tree, remote_tree) = tokio::try_join!(
            self.local.build_merkle(table, range_start, range_end),
            remote.build_merkle(table, range_start, range_end),
        )?;

        let divergent_leaves = local_tree.divergent_leaf_ranges(&remote_tree);
        if divergent_leaves.is_empty() {
            // Replicas already agree — no partition data crosses
            // the wire.
            return Ok(SessionStats::default());
        }

        // Phase 2 — collapse adjacent divergent leaves into
        // maximal contiguous spans so one fetch covers many leaves
        // when the diff is dense. With TREE_DEPTH=15 each leaf
        // covers a thin slice of the token space, but on a
        // newly-rebuilt replica the diff is often a near-contiguous
        // block. Merging here turns N tiny fetches into ~1 wider
        // one without enlarging the working set beyond what the
        // existing 10 000-partition read cap already bounds.
        let merged_spans = merge_contiguous_token_ranges(&divergent_leaves);

        // Phase 3 — per-span streaming fetch + diff + apply.
        //
        // Each span walks with parallel cursors on local and
        // remote (chunked Fetch RPC). Within each chunk, the
        // streaming diff `diff_partition_sets_streaming` consumes
        // both `Vec<Partition>`s and emits one `RepairDecision`
        // per partition — owned, not cloned. The executor pushes
        // each decision into a small per-direction apply queue
        // capped at `REPAIR_APPLY_CHUNK_PARTITIONS`, and flushes
        // the queue via `apply_partitions` whenever it fills (or
        // at span end).
        //
        // Peak in-flight memory per session is bounded by:
        //
        //   local_chunk (≤REPAIR_FETCH_CHUNK_PARTITIONS partitions)
        // + remote_chunk (≤REPAIR_FETCH_CHUNK_PARTITIONS partitions)
        // + a_to_b queue (≤REPAIR_APPLY_CHUNK_PARTITIONS partitions)
        // + b_to_a queue (≤REPAIR_APPLY_CHUNK_PARTITIONS partitions)
        //
        // Critically, the diff itself never materialises a full
        // `RepairPlan` — chosen partitions move directly from
        // the input vecs into the apply queues without an
        // intermediate Vec<Partition> clone (which the legacy
        // `compute_repair_plan` did, costing ~chunk_size ×
        // partition_size of extra allocation per chunk).
        let mut streamed_in: u64 = 0;
        let mut streamed_out: u64 = 0;
        let mut ties: u64 = 0;
        for (span_start, span_end) in merged_spans {
            let mut local_cursor: Option<i64> = None;
            let mut remote_cursor: Option<i64> = None;
            // Per-span apply queues. Sized at
            // `REPAIR_APPLY_CHUNK_PARTITIONS` so each flush
            // sends one Apply RPC body of bounded size.
            let mut a_to_b_queue: Vec<Partition> =
                Vec::with_capacity(REPAIR_APPLY_CHUNK_PARTITIONS);
            let mut b_to_a_queue: Vec<Partition> =
                Vec::with_capacity(REPAIR_APPLY_CHUNK_PARTITIONS);

            loop {
                let (local_res, remote_res) = tokio::try_join!(
                    self.local.read_range_chunked(
                        table,
                        span_start,
                        span_end,
                        local_cursor,
                        REPAIR_FETCH_CHUNK_PARTITIONS,
                    ),
                    remote.read_range_chunked(
                        table,
                        span_start,
                        span_end,
                        remote_cursor,
                        REPAIR_FETCH_CHUNK_PARTITIONS,
                    ),
                )?;
                let (local_parts, local_next) = local_res;
                let (remote_parts, remote_next) = remote_res;
                if local_parts.is_empty() && remote_parts.is_empty() {
                    break;
                }
                // Pick `sub_end` = smallest cursor frontier so the
                // merge-join sees matching sets; anything beyond
                // that on either side stays for the next iteration.
                let local_high = local_next.unwrap_or(span_end);
                let remote_high = remote_next.unwrap_or(span_end);
                let sub_end = local_high.min(remote_high).min(span_end);
                // Filter each side to partitions strictly before
                // `sub_end` (the rest will be re-read on the next
                // pass after the lagging side catches up).
                let in_window = |p: &Partition| p.key.token.0 < sub_end;
                let (mut local_in, local_keep): (Vec<Partition>, Vec<Partition>) =
                    local_parts.into_iter().partition(in_window);
                let (mut remote_in, remote_keep): (Vec<Partition>, Vec<Partition>) =
                    remote_parts.into_iter().partition(in_window);
                // Pre-sorted invariant from the chunked Fetch
                // handler — but we partitioned the windowed half
                // out, so re-establish the order.
                local_in.sort_by_key(|p| (p.key.token.0, p.key.key.as_bytes().to_vec()));
                remote_in.sort_by_key(|p| (p.key.token.0, p.key.key.as_bytes().to_vec()));

                // Streaming diff — partitions move directly into
                // the apply queues. When a queue fills, flush.
                super::diff_partition_sets_streaming(local_in, remote_in, |decision| {
                    match decision {
                        super::RepairDecision::AToB(p) => {
                            a_to_b_queue.push(p);
                        }
                        super::RepairDecision::BToA(p) => {
                            b_to_a_queue.push(p);
                        }
                        super::RepairDecision::Tie(_) => {
                            ties += 1;
                        }
                    }
                    Ok(())
                })?;
                if a_to_b_queue.len() >= REPAIR_APPLY_CHUNK_PARTITIONS {
                    streamed_out += a_to_b_queue.len() as u64;
                    remote.apply_partitions(table, &a_to_b_queue).await?;
                    a_to_b_queue.clear();
                }
                if b_to_a_queue.len() >= REPAIR_APPLY_CHUNK_PARTITIONS {
                    streamed_in += b_to_a_queue.len() as u64;
                    self.local.apply_partitions(table, &b_to_a_queue).await?;
                    b_to_a_queue.clear();
                }
                // Carry the "future" half forward in the input
                // vecs so we don't pay a re-read for it.
                let _ = (local_keep, remote_keep);

                // Advance whichever side hasn't already passed
                // `sub_end`; the other side stays at its cursor.
                local_cursor = match local_next {
                    Some(c) if c <= sub_end => Some(c),
                    _ => local_cursor,
                };
                remote_cursor = match remote_next {
                    Some(c) if c <= sub_end => Some(c),
                    _ => remote_cursor,
                };
                if local_next.is_none() && remote_next.is_none() {
                    break;
                }
            }
            // End-of-span flush.
            if !a_to_b_queue.is_empty() {
                streamed_out += a_to_b_queue.len() as u64;
                remote.apply_partitions(table, &a_to_b_queue).await?;
                a_to_b_queue.clear();
            }
            if !b_to_a_queue.is_empty() {
                streamed_in += b_to_a_queue.len() as u64;
                self.local.apply_partitions(table, &b_to_a_queue).await?;
                b_to_a_queue.clear();
            }
        }

        Ok(SessionStats {
            partitions_streamed_in: streamed_in,
            partitions_streamed_out: streamed_out,
            timestamp_ties: ties,
        })
    }
}

/// Coalesce up to `MERGED_SPAN_MAX_LEAVES` adjacent
/// divergent-leaf ranges into a single span. Bigger spans amortise
/// the per-fetch RPC overhead when the diff is dense; the cap
/// keeps per-span working-set memory bounded — at worst-case
/// uniform partition density the largest span holds
/// `MERGED_SPAN_MAX_LEAVES × partitions_per_leaf` partitions on
/// each side. Without the cap, a fully-divergent table collapses
/// to one giant span and the executor materialises the whole
/// replica per session, defeating the point of Merkle exchange.
fn merge_contiguous_token_ranges(ranges: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut out: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
    let mut run_len: usize = 0;
    for &(s, e) in ranges {
        if let Some(last) = out.last_mut() {
            if last.1 == s && run_len < MERGED_SPAN_MAX_LEAVES {
                last.1 = e;
                run_len += 1;
                continue;
            }
        }
        out.push((s, e));
        run_len = 1;
    }
    out
}

/// Maximum number of adjacent Merkle leaves combined into a
/// single read+apply span. Sized small enough that worst-case
/// partition size (multi-MB embedding rows on the fmem
/// entity_store) keeps a single span's working set well under
/// the per-session memory budget, while still amortising RPC
/// overhead across a useful number of leaves.
pub const MERGED_SPAN_MAX_LEAVES: usize = 8;

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

    async fn build_merkle(
        &self,
        _table: &TableId,
        range_start: i64,
        range_end: i64,
    ) -> Result<super::merkle::MerkleTree, String> {
        let store = self.inner.lock().await;
        let mut tree = super::merkle::MerkleTree::new(super::TREE_DEPTH, range_start, range_end);
        for (_, partition) in store.iter() {
            let token = partition.key.token.0;
            if token < range_start || token >= range_end {
                continue;
            }
            tree.insert(token, super::partition_merkle_hash(partition));
        }
        tree.compute_root();
        Ok(tree)
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

        let executor = LocalRepairExecutor {
            local: a.clone() as Arc<dyn RepairStore>,
            remotes: [(7u64, b.clone() as Arc<dyn RepairStore>)]
                .into_iter()
                .collect(),
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

        let executor = LocalRepairExecutor {
            local: a.clone() as Arc<dyn RepairStore>,
            remotes: [(7u64, b.clone() as Arc<dyn RepairStore>)]
                .into_iter()
                .collect(),
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

    // ---- Three-store integration test exercising coordinator → executor ----

    /// Stand up three [`InMemoryRepairStore`]s as a simulated 3-node, RF=3
    /// cluster, stage the fmem-style divergence pattern (one node has all
    /// the data, the others have partial or none), and run the full
    /// [`crate::repair::RepairCoordinator`] against the executor. After
    /// the run, every store must hold the same set of partitions and the
    /// values must reflect the highest-timestamp source.
    #[tokio::test]
    async fn coordinator_plus_local_executor_converges_three_node_cluster() {
        use crate::raft::{NodeInfo, NodeState};
        use crate::repair::RepairCoordinator;
        use crate::ring::TokenRing;
        use uuid::Uuid;

        fn node_with(addr: &str) -> NodeInfo {
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: addr.to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            }
        }

        // 3-node ring, RF=3 — every range owned by every node.
        let mut ring = TokenRing::new();
        ring.add_node(1, node_with("10.0.0.1:7000"));
        ring.add_node(2, node_with("10.0.0.2:7000"));
        ring.add_node(3, node_with("10.0.0.3:7000"));
        ring.assign_tokens(1, &[100_000_000]);
        ring.assign_tokens(2, &[200_000_000]);
        ring.assign_tokens(3, &[300_000_000]);

        // node1: full dataset, node2: partial (3 of 5 keys), node3: empty.
        // Mirrors the fmem scenario (node1=1.41GB, node2=401MB, node3=217MB
        // is the same shape — just shrunk to 5 keys for the test).
        let store1 = Arc::new(InMemoryRepairStore::new());
        let store2 = Arc::new(InMemoryRepairStore::new());
        let store3 = Arc::new(InMemoryRepairStore::new());

        let tokens = [
            (50_000_000, b"k1"),
            (150_000_000, b"k2"),
            (250_000_000, b"k3"),
            (350_000_000, b"k4"),
            (450_000_000, b"k5"),
        ];
        for (token, key) in tokens.iter() {
            let p = test_partition_at(*token, key.as_ref(), b"v", 1_000);
            store1.insert(p.clone()).await;
            // node2 has only k1, k2, k3
            if matches!(*key, b"k1" | b"k2" | b"k3") {
                store2.insert(p.clone()).await;
            }
            // node3 has nothing
        }

        // Run repair FROM node1's perspective: local=store1, remotes=store2+store3.
        let executor = Arc::new(LocalRepairExecutor {
            local: store1.clone() as Arc<dyn RepairStore>,
            remotes: [
                (2u64, store2.clone() as Arc<dyn RepairStore>),
                (3u64, store3.clone() as Arc<dyn RepairStore>),
            ]
            .into_iter()
            .collect(),
        });
        let coord = RepairCoordinator::default();
        let results = coord
            .repair_table(executor, &ring, 1, 3, &TableId::new("ks", "tbl"))
            .await;
        assert!(!results.is_empty(), "coordinator must dispatch sessions");
        assert!(
            results.iter().all(|r| r.result.is_ok()),
            "every session must succeed; got errors: {:?}",
            results
                .iter()
                .filter_map(|r| r.result.as_ref().err())
                .collect::<Vec<_>>()
        );

        // After repair, all three stores hold the SAME set of partition keys.
        let want: std::collections::BTreeSet<Vec<u8>> =
            tokens.iter().map(|(_, k)| k.to_vec()).collect();
        for (name, store) in [("node1", &store1), ("node2", &store2), ("node3", &store3)] {
            let snap = store.snapshot().await;
            let got: std::collections::BTreeSet<Vec<u8>> = keys_in(&snap);
            assert_eq!(
                got, want,
                "{name} did not converge: got {got:?} want {want:?}"
            );
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

        let executor = LocalRepairExecutor {
            local: a.clone() as Arc<dyn RepairStore>,
            remotes: [(7u64, b.clone() as Arc<dyn RepairStore>)]
                .into_iter()
                .collect(),
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

#[cfg(test)]
mod streaming_contract_tests {
    #[test]
    fn repair_leaf_reads_must_not_use_fixed_materialization_cap() {
        let source = include_str!("executor.rs");
        let read_range_body = source
            .split("impl RepairStore for StorageEngineRepairStore")
            .nth(1)
            .expect("StorageEngineRepairStore impl must be present")
            .split("async fn read_range(\n        &self,\n        table: &TableId,")
            .nth(1)
            .and_then(|rest| rest.split("async fn read_range_chunked").next())
            .expect("StorageEngineRepairStore read_range body must be present");
        assert!(
            !read_range_body.contains("REPAIR_LEAF_READ_LIMIT"),
            "repair must stream token ranges or fail loudly instead of using a fixed materialization cap"
        );
        assert!(
            read_range_body.contains("read_range_chunked"),
            "one-shot repair reads must be implemented as a chunked loop over the bounded repair cursor"
        );
    }
}
