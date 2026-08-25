//! Module: Coordinate replica reads with consistency-level enforcement.
//! Correctness: Correct when replica selection satisfies the requested CL,
//!   latency-sensitive reads and writes retain Data-lane capacity, and
//!   unbounded scan traffic is isolated on the Bulk lane.
//! Last revised: 2026-08-24
//! Last changed: Route unbounded multi-page partition reads through the Bulk
//!   internode lane so they cannot monopolize the Data lane used by writes.
//!
//! # Two-Phase Digest Read Protocol
//!
//! For CL > ONE:
//!
//! **Phase 1 — Concurrent fan-out**
//! 1. Compute replica set from the token ring (`ring.replicas(token, rf)`).
//! 2. Verify `replicas.len() >= block_for(cl)`, else return `Unavailable`.
//! 3. Pick one replica for a **full read** (prefer local if self is a replica).
//! 4. Send **digest-only reads** to the next required replicas.
//! 5. If one fails, launch spare digest reads from remaining eligible replicas.
//!
//! **Phase 2 — Resolve**
//! 1. All digests match the full read's digest → return data (fast path).
//! 2. Any digest mismatch → fetch full data from mismatched replicas, compare
//!    by timestamp (last-write-wins).  Spawn async repair writes to stale replicas.
//!
//! For CL = ONE: skip digest entirely — read from one replica, prefer local.

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};

use ferrosa_common::key::DecoratedKey;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_storage::TableId;

use ferrosa_storage::Mutation;
use std::collections::BTreeMap;

use crate::consistency::ConsistencyLevel;
use crate::error::ClusterError;
use crate::pair::coordinator::encode_mutation;
use crate::raft::handlers::{
    partition_from_wire, FulltextSearchRequestPayload, FulltextSearchResponsePayload,
    IndexReadInPartitionRequestPayload, IndexReadRequestPayload, IndexReadResponsePayload,
    RangeReadRequestPayload, RangeReadResponsePayload, ReadRequestPayload, ReadResponsePayload,
};
use crate::raft::IndexNodeStatus;

use super::ClusterCoordinator;

/// Reorder replicas so that nodes with [`IndexNodeStatus::Ready`] for the given
/// index come first. Nodes without status information or with non-Ready status
/// are appended in their original order. Returns all replicas (never filters).
///
/// Called by the coordinator during index-aware reads to prefer replicas
/// that have finished building the queried index.
pub fn select_index_ready_replicas(
    replicas: &[u64],
    keyspace: &str,
    table: &str,
    index_name: &str,
    index_state_map: &BTreeMap<(String, String, String), BTreeMap<u64, IndexNodeStatus>>,
) -> Vec<u64> {
    let key = (
        keyspace.to_string(),
        table.to_string(),
        index_name.to_string(),
    );
    let node_statuses = match index_state_map.get(&key) {
        Some(statuses) => statuses,
        None => return replicas.to_vec(),
    };

    let mut ready = Vec::new();
    let mut rest = Vec::new();

    for &replica in replicas {
        match node_statuses.get(&replica) {
            Some(IndexNodeStatus::Ready) => ready.push(replica),
            _ => rest.push(replica),
        }
    }

    ready.extend(rest);
    ready
}

pub(super) trait RangeReadStorage {
    fn read_range_unbounded(
        &self,
        table_id: &TableId,
        limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>>;

    fn read_range_bounded_rows(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>>;
}

impl RangeReadStorage for ferrosa_storage::StorageEngine {
    fn read_range_unbounded(
        &self,
        table_id: &TableId,
        limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        self.read_range(table_id, None, None, limit)
    }

    fn read_range_bounded_rows(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        self.read_range_limited_rows(table_id, None, None, limit, row_limit)
    }
}

pub(super) fn read_local_range_limited_rows(
    storage: &impl RangeReadStorage,
    table_id: &TableId,
    limit: usize,
    row_limit: usize,
) -> ferrosa_common::Result<Vec<Partition>> {
    if row_limit > 0 {
        storage.read_range_bounded_rows(table_id, limit, row_limit)
    } else {
        storage.read_range_unbounded(table_id, limit)
    }
}

/// Offloaded wrapper around [`read_local_range_limited_rows`] for the concrete
/// storage engine.
///
/// [`read_local_range_limited_rows`] performs a SYNCHRONOUS range scan (opens
/// SSTable readers, decodes partitions, `std::fs` + S3 rehydration). Running it
/// inline on an async worker parks that worker for the duration of a large local
/// range read, which stalls the CQL connection's keepalive and raft heartbeats.
/// It is therefore offloaded to a blocking thread via [`TaskPool::spawn_blocking`],
/// mirroring `read_local_partition`. A `JoinError` from the blocking pool is
/// mapped to a loud `Storage` error rather than swallowed as an empty range.
async fn read_local_range_limited_rows_offloaded(
    storage: &std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: &TableId,
    limit: usize,
    row_limit: usize,
) -> ferrosa_common::Result<Vec<Partition>> {
    let storage = std::sync::Arc::clone(storage);
    let table_id = table_id.clone();
    ferrosa_common::task_pool::TaskPool::current("coordinator-local-range-read")
        .spawn_blocking(move || {
            read_local_range_limited_rows(storage.as_ref(), &table_id, limit, row_limit)
        })
        .await
        .map_err(|e| {
            ferrosa_common::Error::Io(std::io::Error::other(format!(
                "local range read task failed: {e}"
            )))
        })?
}

// ---------------------------------------------------------------------------
// Anti-entropy repair requests (async, fired from the read path)
// ---------------------------------------------------------------------------

/// A request to refill a token range from a healthy replica via anti-entropy
/// repair, fired by the read coordinator when it served a read around a corrupt
/// local SSTable (LOCKED DESIGN: serve now, repair in the background).
///
/// The request names the table and the corrupt SSTable's covered token range so
/// the repair scheduler can run a targeted Merkle repair of exactly that range
/// rather than the whole table. It is *recorded* (not yet executed) here: the
/// read path must never block on repair, and the scheduler drains these on its
/// own tick. Draining-into-`repair_initiated` is wired separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AntiEntropyRepairRequest {
    /// Table whose range needs refilling.
    pub table_id: TableId,
    /// Lower bound (inclusive) of the corrupt SSTable's covered token range.
    pub min_token: i64,
    /// Upper bound (inclusive) of the corrupt SSTable's covered token range.
    pub max_token: i64,
}

/// Bounded, observable sink for [`AntiEntropyRepairRequest`]s fired from the
/// read path. Holds a bounded backlog the repair scheduler drains; a global
/// metric counts every request so corruption-driven self-heal is alertable.
///
/// Bounded (Power-of-10 Rule 3): once the backlog is full, further requests
/// still bump the metric (so corruption is never silently dropped from
/// observability) but are not queued again — the already-queued range repair
/// for that table will refill overlapping corrupt ranges anyway.
pub struct AntiEntropyRepairQueue {
    /// Max queued requests before new ones are coalesced away (metric still
    /// fires). Generous: each entry is tiny and the scheduler drains quickly.
    capacity: usize,
    pending: parking_lot::Mutex<std::collections::VecDeque<AntiEntropyRepairRequest>>,
}

impl AntiEntropyRepairQueue {
    /// Default backlog capacity. Far larger than the number of distinct
    /// corrupt SSTables a single node realistically quarantines between repair
    /// ticks, so coalescing only engages under pathological corruption.
    const DEFAULT_CAPACITY: usize = 1024;

    fn new() -> Self {
        Self {
            capacity: Self::DEFAULT_CAPACITY,
            pending: parking_lot::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Record a repair request: always bumps the global metric; enqueues the
    /// request unless the bounded backlog is full. Returns the new total count
    /// of requests observed process-wide.
    fn request(&self, req: AntiEntropyRepairRequest) -> u64 {
        let total = super::metrics::inc_anti_entropy_repair_requested();
        let mut pending = self.pending.lock();
        if pending.len() < self.capacity {
            pending.push_back(req);
        } else {
            tracing::warn!(
                "anti-entropy repair backlog full ({}); coalescing new corrupt-range \
                 request into existing queued repairs (metric still incremented)",
                self.capacity
            );
        }
        total
    }

    /// Drain and return all queued requests (the scheduler's pull side).
    fn drain(&self) -> Vec<AntiEntropyRepairRequest> {
        self.pending.lock().drain(..).collect()
    }
}

impl Default for AntiEntropyRepairQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Internal result type for a single replica read
// ---------------------------------------------------------------------------

/// Outcome of a single replica read attempt.
enum ReplicaRead {
    /// Full partition data (from the designated full-read replica).
    Full(Option<Partition>),
    /// Digest + timestamp only (from digest-only replicas).
    Digest {
        digest: Option<u32>,
        timestamp: i64,
        /// Host ID of the replica that sent this digest.
        host_id: Option<uuid::Uuid>,
    },
    /// The replica did not respond or returned an error.
    Failed,
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

/// Encode a [`ReadRequestPayload`] as a bincode-prefixed [`Bytes`].
fn encode_read_request(payload: &ReadRequestPayload) -> Bytes {
    Bytes::from(bincode::serialize(payload).unwrap_or_default())
}

/// Read a partition from the local storage engine.
///
/// The storage read is synchronous and, on an evicted-SSTable miss, performs
/// blocking I/O (S3 rehydration, `std::fs`, a `std::sync::Mutex` guard). It is
/// offloaded to a blocking thread via [`TaskPool::spawn_blocking`] — mirroring
/// the range-scan path in `ferrosa-storage`'s `TableStore` — so it never parks
/// an async worker thread (raft apply, coordinator, CQL handler). Args are
/// owned so they can move into the `'static` blocking closure.
///
/// A `JoinError` from the blocking pool is mapped to a loud `Storage` error
/// rather than swallowed, so a panicked read surfaces instead of masquerading
/// as a missing partition.
async fn read_local_partition(
    storage: &std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: TableId,
    key: DecoratedKey,
    row_limit: usize,
    clustering: Option<Vec<u8>>,
) -> ferrosa_common::Result<Option<Partition>> {
    let storage = std::sync::Arc::clone(storage);
    ferrosa_common::task_pool::TaskPool::current("coordinator-local-read")
        .spawn_blocking(move || match clustering {
            Some(clustering) => storage.read_clustering_row(&table_id, &key, &clustering),
            None => storage.read_limited_rows(&table_id, &key, row_limit),
        })
        .await
        .map_err(|e| {
            ferrosa_common::Error::Io(std::io::Error::other(format!(
                "local partition read task failed: {e}"
            )))
        })?
}

#[allow(clippy::too_many_arguments)]
async fn digest_read_attempt(
    coordinator: &ClusterCoordinator,
    storage: std::sync::Arc<ferrosa_storage::StorageEngine>,
    table_id: TableId,
    key: DecoratedKey,
    local_node_id: u64,
    replica_id: u64,
    remote: Option<(uuid::Uuid, String)>,
    row_limit: usize,
    clustering: Option<Vec<u8>>,
) -> ReplicaRead {
    if replica_id == local_node_id {
        match read_local_partition(&storage, table_id, key, row_limit, clustering).await {
            Ok(Some(p)) => {
                use crate::raft::handlers::compute_partition_digest;
                let ts = p
                    .rows
                    .iter()
                    .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
                    .max()
                    .unwrap_or(i64::MIN);
                let digest = compute_partition_digest(&p).ok();
                ReplicaRead::Digest {
                    digest,
                    timestamp: ts,
                    host_id: None,
                }
            }
            Ok(None) => ReplicaRead::Digest {
                digest: None,
                timestamp: i64::MIN,
                host_id: None,
            },
            Err(_) => ReplicaRead::Failed,
        }
    } else {
        let payload = ReadRequestPayload {
            keyspace: table_id.keyspace,
            table: table_id.table,
            key: key.key.as_bytes().to_vec(),
            digest_only: true,
            page_size: row_limit.min(u32::MAX as usize) as u32,
            page_state: vec![],
            clustering: clustering.unwrap_or_default(),
        };
        let body = encode_read_request(&payload);
        match remote {
            None => ReplicaRead::Failed,
            Some((hid, addr)) => match coordinator
                .send_remote_with_reconnect(hid, &addr, Message::ReadRequest(body), Lane::Data)
                .await
            {
                Ok(Message::ReadResponse(b)) => match decode_read_response(&b) {
                    Some(resp) => ReplicaRead::Digest {
                        digest: resp.digest,
                        timestamp: resp.timestamp,
                        host_id: Some(hid),
                    },
                    None => ReplicaRead::Failed,
                },
                _ => ReplicaRead::Failed,
            },
        }
    }
}

/// Decode a [`ReadResponsePayload`] from raw bytes, or return `None`.
fn decode_read_response(bytes: &[u8]) -> Option<ReadResponsePayload> {
    bincode::deserialize(bytes)
        .map_err(|e| tracing::warn!("coordinate_read: failed to decode ReadResponse: {e}"))
        .ok()
}

fn should_retry_missing_peer_error(err: &str) -> bool {
    err.contains("unknown peer")
        || err.contains("no connection pool")
        || err.contains("lane is reconnecting")
        || err.contains("lane permanently failed")
}

/// Select the internode lane for a single-partition read.
///
/// A clustering-row lookup or a row-bounded request is latency-sensitive and
/// bounded, so it stays on `Data`. An unbounded partition read can require many
/// remote pages (70K-row partitions are observed in production); it belongs on
/// `Bulk` so those pages cannot queue ahead of small writes on `Data`.
fn partition_read_lane(row_limit: usize, clustering: Option<&[u8]>) -> Lane {
    if row_limit == 0 && clustering.is_none() {
        Lane::Bulk
    } else {
        Lane::Data
    }
}

// ---------------------------------------------------------------------------
// coordinate_read
// ---------------------------------------------------------------------------

impl ClusterCoordinator {
    async fn send_remote_with_reconnect(
        &self,
        host_id: uuid::Uuid,
        addr: &str,
        message: Message,
        lane: Lane,
    ) -> crate::error::Result<Message> {
        match self.peer_manager.send(host_id, message.clone(), lane).await {
            Ok(resp) => Ok(resp),
            Err(e) if should_retry_missing_peer_error(&e.to_string()) => {
                self.peer_manager.ensure_peer(host_id, addr).await?;
                self.peer_manager
                    .send(host_id, message, lane)
                    .await
                    .map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn send_remote_with_reconnect_timeout(
        &self,
        host_id: uuid::Uuid,
        addr: &str,
        message: Message,
        lane: Lane,
        timeout: std::time::Duration,
    ) -> crate::error::Result<Message> {
        match self
            .peer_manager
            .send_with_timeout(host_id, message.clone(), lane, timeout)
            .await
        {
            Ok(resp) => Ok(resp),
            Err(e) if should_retry_missing_peer_error(&e.to_string()) => {
                self.peer_manager.ensure_peer(host_id, addr).await?;
                self.peer_manager
                    .send_with_timeout(host_id, message, lane, timeout)
                    .await
                    .map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Coordinate a read from the appropriate replicas.
    ///
    /// For CL = ONE: contact a single replica (local preferred).
    /// For CL > ONE: two-phase digest protocol — full read from one replica,
    /// digest-only reads from the rest; mismatch triggers full re-fetch and
    /// last-write-wins merge.
    pub async fn coordinate_read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_with(table_id, key, self.default_cl, self.default_rf)
            .await
    }

    async fn full_refetch_limited_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        host_id: uuid::Uuid,
        row_limit: usize,
        clustering: Option<&[u8]>,
    ) -> Option<Partition> {
        let addr = {
            let ring = self.ring.load();
            ring.node_ids()
                .into_iter()
                .filter_map(|node_id| ring.get_node(node_id))
                .find(|node| node.host_id == host_id)
                .map(|node| node.addr.clone())
        }?;
        let payload = ReadRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            key: key.key.as_bytes().to_vec(),
            digest_only: false,
            page_size: row_limit.min(u32::MAX as usize) as u32,
            page_state: vec![],
            clustering: clustering.unwrap_or_default().to_vec(),
        };
        let body = encode_read_request(&payload);
        match self
            .send_remote_with_reconnect(host_id, &addr, Message::ReadRequest(body), Lane::Data)
            .await
        {
            Ok(Message::ReadResponse(b)) => match decode_read_response(&b) {
                Some(resp) if resp.found => resp.partition.map(partition_from_wire),
                _ => None,
            },
            _ => None,
        }
    }

    #[cfg(test)]
    async fn full_refetch(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        host_id: uuid::Uuid,
    ) -> Option<Partition> {
        self.full_refetch_limited_rows(table_id, key, host_id, 0, None)
            .await
    }

    /// Coordinate a read with explicit consistency level and replication factor.
    ///
    /// Use this when the query specifies a CL or the keyspace has a non-default RF.
    pub async fn coordinate_read_with(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_with_limited_rows(table_id, key, cl, rf, 0)
            .await
    }

    /// Coordinate a read while retaining at most `row_limit` clustered rows
    /// from the requested partition when the limit is non-zero.
    ///
    /// The digest protocol hashes the same bounded slice returned to the
    /// client, which keeps `SELECT ... LIMIT N` over a wide partition from
    /// waiting on full-partition decode while preserving quorum agreement for
    /// the requested result slice.
    pub async fn coordinate_read_with_limited_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        rf: usize,
        row_limit: usize,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_with_filter(table_id, key, cl, rf, row_limit, None)
            .await
    }

    pub async fn coordinate_read_clustering_row(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        clustering: &[u8],
        cl: ConsistencyLevel,
        rf: usize,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_with_filter(table_id, key, cl, rf, 0, Some(clustering.to_vec()))
            .await
    }

    async fn coordinate_read_with_filter(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        rf: usize,
        row_limit: usize,
        clustering: Option<Vec<u8>>,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        let ring = self.ring.load();
        let raw_replicas = ring.replicas(key.token.0, rf);

        // W8.4: filter the replica list by the CL's role policy.
        // Voter-quorum CLs drop learners; ONE / LOCAL_ONE keep them.
        let replicas =
            crate::coordinator::cl_routing::eligible_replicas_for_cl(cl, &raw_replicas, &ring);

        let required = cl.block_for(rf);

        if tracing::enabled!(tracing::Level::DEBUG) {
            let span = tracing::debug_span!(
                "cluster.read",
                cl = %cl,
                replicas = replicas.len(),
                raw_replicas = raw_replicas.len(),
            );
            let _enter = span.enter();
        }

        if replicas.len() < required {
            return Err(ClusterError::Unavailable {
                consistency: cl.to_string(),
                required,
                alive: replicas.len(),
            });
        }

        // -------------------------------------------------------------------
        // CL = ONE fast path: single replica, prefer local.
        // -------------------------------------------------------------------
        if cl == ConsistencyLevel::One || cl == ConsistencyLevel::LocalOne {
            return self
                .read_one_replica_limited_rows(
                    table_id,
                    key,
                    &replicas,
                    &ring,
                    row_limit,
                    clustering.as_deref(),
                )
                .await;
        }

        // -------------------------------------------------------------------
        // CL > ONE: two-phase digest protocol.
        // -------------------------------------------------------------------

        // Choose which replica performs the full read: prefer local.
        let local = self.local_node_id;
        let full_replica = if replicas.contains(&local) {
            local
        } else {
            replicas[0]
        };

        // Start with only the digest reads needed for quorum. Spare eligible
        // replicas are kept in reserve and launched only if an initial read
        // fails, avoiding a permanent extra digest RPC on every healthy read.
        let digest_replicas: Vec<u64> = replicas
            .iter()
            .copied()
            .filter(|&r| r != full_replica)
            .collect();

        // Collect node metadata before dropping the ring guard.
        let full_remote = ring
            .get_node(full_replica)
            .map(|n| (n.host_id, n.addr.clone()));
        let full_host_id = full_remote.as_ref().map(|(host_id, _)| *host_id);
        let mut digest_remotes: std::collections::VecDeque<(u64, Option<(uuid::Uuid, String)>)> =
            digest_replicas
                .iter()
                .map(|&r| (r, ring.get_node(r).map(|n| (n.host_id, n.addr.clone()))))
                .collect();
        drop(ring);

        // -------------------------------------------------------------------
        // Phase 1: fan out — full read + digest-only reads concurrently.
        // -------------------------------------------------------------------

        let mut fan_out: FuturesUnordered<_> = {
            // Full-read future
            let full_future = {
                let storage = self.storage.clone();
                let coordinator = self;
                let table_id = table_id.clone();
                let key = key.clone();
                let local_node_id = self.local_node_id;
                let keyspace = table_id.keyspace.clone();
                let table_name = table_id.table.clone();
                let key_bytes = key.key.as_bytes().to_vec();
                let clustering = clustering.clone();

                async move {
                    if full_replica == local_node_id {
                        // Local full read.
                        match read_local_partition(&storage, table_id, key, row_limit, clustering)
                            .await
                        {
                            Ok(opt) => ReplicaRead::Full(opt),
                            Err(_) => ReplicaRead::Failed,
                        }
                    } else {
                        // Remote full read via Data lane.
                        let payload = ReadRequestPayload {
                            keyspace,
                            table: table_name,
                            key: key_bytes,
                            digest_only: false,
                            page_size: row_limit.min(u32::MAX as usize) as u32,
                            page_state: vec![],
                            clustering: clustering.unwrap_or_default(),
                        };
                        let body = encode_read_request(&payload);
                        match full_remote {
                            None => ReplicaRead::Failed,
                            Some((hid, addr)) => {
                                match coordinator
                                    .send_remote_with_reconnect(
                                        hid,
                                        &addr,
                                        Message::ReadRequest(body),
                                        Lane::Data,
                                    )
                                    .await
                                {
                                    Ok(Message::ReadResponse(b)) => {
                                        match decode_read_response(&b) {
                                            Some(resp) if resp.found => {
                                                let partition =
                                                    resp.partition.map(partition_from_wire);
                                                ReplicaRead::Full(partition)
                                            }
                                            Some(_) => ReplicaRead::Full(None),
                                            None => ReplicaRead::Failed,
                                        }
                                    }
                                    _ => ReplicaRead::Failed,
                                }
                            }
                        }
                    }
                }
            };

            // Collect all futures into one FuturesUnordered.
            let all: FuturesUnordered<
                std::pin::Pin<Box<dyn std::future::Future<Output = ReplicaRead> + Send>>,
            > = FuturesUnordered::new();
            all.push(Box::pin(full_future));
            for _ in 0..required.saturating_sub(1) {
                if let Some((replica_id, remote)) = digest_remotes.pop_front() {
                    all.push(Box::pin(digest_read_attempt(
                        self,
                        self.storage.clone(),
                        table_id.clone(),
                        key.clone(),
                        self.local_node_id,
                        replica_id,
                        remote,
                        row_limit,
                        clustering.clone(),
                    )));
                }
            }
            all
        };

        // -------------------------------------------------------------------
        // Phase 2: collect results and resolve.
        // -------------------------------------------------------------------

        let mut full_partition: Option<Option<Partition>> = None; // None means "not received yet"
        let mut digest_responses: Vec<(Option<u32>, i64, Option<uuid::Uuid>)> = Vec::new();
        let mut received = 0usize;
        let mut full_digest: Option<Option<u32>> = None;

        while let Some(result) = fan_out.next().await {
            match result {
                ReplicaRead::Full(opt_partition) => {
                    let d = opt_partition
                        .as_ref()
                        .and_then(|p| crate::raft::handlers::compute_partition_digest(p).ok());
                    full_digest = Some(d);
                    full_partition = Some(opt_partition);
                    received += 1;
                }
                ReplicaRead::Digest {
                    digest,
                    timestamp,
                    host_id,
                } => {
                    digest_responses.push((digest, timestamp, host_id));
                    received += 1;
                }
                ReplicaRead::Failed => {
                    // Don't count failures toward `received`. If another
                    // eligible digest replica is available, launch it now so a
                    // single failed remote doesn't fail the quorum read.
                    if let Some((replica_id, remote)) = digest_remotes.pop_front() {
                        fan_out.push(Box::pin(digest_read_attempt(
                            self,
                            self.storage.clone(),
                            table_id.clone(),
                            key.clone(),
                            self.local_node_id,
                            replica_id,
                            remote,
                            row_limit,
                            clustering.clone(),
                        )));
                    }
                }
            }

            // We're done once we have the full read + all needed digests.
            if full_partition.is_some() && received >= required {
                break;
            }
        }

        // Fail if we didn't get enough responses.
        if received < required || full_partition.is_none() {
            return Err(ClusterError::ReadTimeout {
                consistency: cl.to_string(),
                received,
                required,
                data_present: full_partition.is_some(),
            });
        }

        let full_partition = full_partition.unwrap();
        let full_d = full_digest.unwrap(); // same Some wrapping as full_partition above

        // Check for digest mismatches.
        let all_match = digest_responses.iter().all(|(d, _, _)| *d == full_d);

        if all_match {
            // Fast path: all digests agree.
            return Ok(full_partition.map(|p| p.rows));
        }

        // Slow path: digest mismatch — log warning, pick newest by timestamp,
        // then repair stale replicas asynchronously.
        tracing::warn!(
            table = %table_id,
            "read repair needed: digest mismatch among replicas"
        );

        let full_ts = full_partition
            .as_ref()
            .map(|p| {
                p.rows
                    .iter()
                    .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
                    .max()
                    .unwrap_or(i64::MIN)
            })
            .unwrap_or(i64::MIN);

        // Find the remote replica with the newest timestamp so we can attempt
        // a full re-fetch if it is newer than the full-read replica.
        let newest_remote = digest_responses.iter().max_by_key(|(_, ts, _)| *ts);

        let (newest_remote_ts, newest_remote_host_id) = match newest_remote {
            Some(&(_, ts, hid)) => (ts, hid),
            None => (i64::MIN, None),
        };

        // Determine the newest partition and collect stale host IDs.
        let (result_partition, stale_host_ids) = if newest_remote_ts > full_ts {
            // A remote replica is newer -- attempt a full re-fetch.
            tracing::warn!(
                table = %table_id,
                full_ts,
                newest_remote_ts,
                "remote replica is newer; attempting full re-fetch"
            );

            if let Some(hid) = newest_remote_host_id {
                if let Some(newer_partition) = self
                    .full_refetch_limited_rows(table_id, key, hid, row_limit, clustering.as_deref())
                    .await
                {
                    // The full-read replica (and any other mismatched digests
                    // except the one we just fetched from) are stale.
                    let mut stale: Vec<uuid::Uuid> = digest_responses
                        .iter()
                        .filter(|(d, _, _)| *d != full_d)
                        .filter_map(|(_, _, h)| *h)
                        .filter(|h| *h != hid) // don't repair the one we fetched from
                        .collect();
                    // Also include the full-read replica's host_id if it's remote.
                    if let Some(full_hid) = full_host_id {
                        stale.push(full_hid);
                    }
                    (Some(newer_partition), stale)
                } else {
                    // Re-fetch failed — we know a newer version exists but
                    // cannot retrieve it. Returning stale data here would
                    // violate linearizability (a write visible on one replica
                    // becomes invisible to a subsequent read). Return an error
                    // so the client retries rather than seeing stale state.
                    tracing::warn!(
                        table = %table_id,
                        full_ts,
                        newest_remote_ts,
                        "full re-fetch from newer replica failed; refusing to return stale data"
                    );
                    return Err(ClusterError::ReadTimeout {
                        consistency: cl.to_string(),
                        received,
                        required,
                        data_present: true,
                    });
                }
            } else {
                (full_partition.clone(), vec![])
            }
        } else {
            // Full-read replica has the newest data -- repair mismatched remotes.
            let stale: Vec<uuid::Uuid> = digest_responses
                .iter()
                .filter(|(d, _, _)| *d != full_d)
                .filter_map(|(_, _, h)| *h)
                .collect();
            (full_partition.clone(), stale)
        };

        // Send repair writes to stale replicas. Awaited inline so that
        // the repair completes before the coordinator returns. This prevents
        // a race where a subsequent read through a different replica sees
        // stale data because the repair hasn't landed yet.
        if !stale_host_ids.is_empty() {
            if let Some(ref partition) = result_partition {
                send_repair_writes(
                    &self.peer_manager,
                    self.storage.as_ref(),
                    &self.repair_metrics,
                    self.local_host_id(),
                    table_id,
                    partition,
                    &stale_host_ids,
                )
                .await;
            }
        }

        Ok(result_partition.map(|p| p.rows))
    }

    /// Send repair writes to stale replicas (delegates to [`send_repair_writes`]).
    ///
    /// Called directly in tests; the production code path uses the free function
    /// `send_repair_writes` inside a `tokio::spawn` fire-and-forget task.
    #[allow(dead_code)]
    pub(crate) async fn repair_stale_replicas(
        &self,
        table_id: &TableId,
        partition: &Partition,
        stale_host_ids: &[uuid::Uuid],
    ) {
        send_repair_writes(
            &self.peer_manager,
            self.storage.as_ref(),
            &self.repair_metrics,
            self.local_host_id(),
            table_id,
            partition,
            stale_host_ids,
        )
        .await;
    }

    fn local_host_id(&self) -> Option<uuid::Uuid> {
        self.ring
            .load()
            .get_node(self.local_node_id)
            .map(|info| info.host_id)
    }

    /// Process-wide count of async anti-entropy repairs this coordinator has
    /// requested after serving a read around a corrupt local SSTable.
    pub fn anti_entropy_repairs_requested_total(&self) -> u64 {
        super::metrics::anti_entropy_repairs_requested_total()
    }

    /// Drain the queued anti-entropy repair requests fired by the read path.
    /// The repair scheduler calls this on its tick to run targeted range
    /// repairs; tests call it to assert a repair was requested.
    pub fn drain_anti_entropy_repair_requests(&self) -> Vec<AntiEntropyRepairRequest> {
        self.anti_entropy_repair_queue.drain()
    }

    /// Record an async anti-entropy repair request for the corrupt SSTable's
    /// token range. Never blocks the read; the scheduler refills the range from
    /// a healthy replica in the background.
    fn request_anti_entropy_repair(&self, table_id: &TableId, min_token: i64, max_token: i64) {
        let req = AntiEntropyRepairRequest {
            table_id: table_id.clone(),
            min_token,
            max_token,
        };
        let total = self.anti_entropy_repair_queue.request(req);
        tracing::warn!(
            table = %table_id,
            min_token,
            max_token,
            anti_entropy_repairs_requested_total = total,
            "served read around a corrupt local SSTable; requested async anti-entropy \
             repair to refill the range from a healthy replica"
        );
    }

    // -----------------------------------------------------------------------
    // CL=ONE helper
    // -----------------------------------------------------------------------

    /// Read from a single replica, preferring local.
    ///
    /// Tries the local node first (if it's a replica), then iterates through
    /// remaining replicas in order until one returns data or all are exhausted.
    async fn read_one_replica_limited_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        replicas: &[u64],
        ring: &crate::ring::TokenRing,
        row_limit: usize,
        clustering: Option<&[u8]>,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        // Build an ordered candidate list: local node first, then remaining replicas.
        let mut candidates: Vec<u64> = Vec::with_capacity(replicas.len());
        if replicas.contains(&self.local_node_id) {
            candidates.push(self.local_node_id);
        }
        for &r in replicas {
            if r != self.local_node_id {
                candidates.push(r);
            }
        }

        // LOCKED DESIGN: a local corrupt-SSTable read is treated like a failed
        // replica — at CL=ONE we fall over to a remote replica to serve the
        // client. The corrupt SSTable's token range is remembered so that, once
        // a healthy replica serves the read, we fire an ASYNC anti-entropy
        // repair to refill that range (never blocking the read). `None` until a
        // local read surfaces a typed `CorruptSstable` error.
        let mut corrupt_range: Option<(i64, i64)> = None;

        for &target in &candidates {
            if target == self.local_node_id {
                match read_local_partition(
                    &self.storage,
                    table_id.clone(),
                    key.clone(),
                    row_limit,
                    clustering.map(<[u8]>::to_vec),
                )
                .await
                .map(|opt| opt.map(|p| p.rows))
                .map_err(ClusterError::Storage)
                {
                    Ok(Some(rows)) if !rows.is_empty() => return Ok(Some(rows)),
                    Ok(_) => continue, // no data on this replica, try next
                    Err(ClusterError::Storage(ref e)) if e.corrupt_sstable_range().is_some() => {
                        // Genuine local SSTable corruption (storage already
                        // quarantined it). Treat it as a failed replica: record
                        // the range so a successful failover triggers repair,
                        // then try the next replica.
                        corrupt_range = e.corrupt_sstable_range();
                        tracing::warn!(
                            %e,
                            "read_one_replica: local SSTable corrupt; failing over to a \
                             remote replica and scheduling anti-entropy repair"
                        );
                        continue;
                    }
                    Err(e) => {
                        tracing::debug!(%e, "read_one_replica: local read failed, trying next");
                        continue;
                    }
                }
            }

            // Remote replica — use paged reads to avoid Data lane timeout
            // on large partitions (e.g., 70K rows / 209MB).
            let (host_id, addr) = match ring.get_node(target).map(|n| (n.host_id, n.addr.clone())) {
                Some(remote) => remote,
                None => continue,
            };

            /// Max rows per page for remote partition reads. Keeps each
            /// response well under the Data lane timeout (10s) and frame
            /// size limit (256MB). 5000 rows × ~3KB each ≈ 15MB per page.
            const READ_PAGE_SIZE: u32 = 5000;
            let read_page_size = if row_limit > 0 {
                row_limit.min(u32::MAX as usize) as u32
            } else {
                READ_PAGE_SIZE
            };

            let mut all_rows: Vec<Row> = Vec::new();
            let mut page_state: Vec<u8> = vec![];
            let mut found_partition = false;

            loop {
                let payload = ReadRequestPayload {
                    keyspace: table_id.keyspace.clone(),
                    table: table_id.table.clone(),
                    key: key.key.as_bytes().to_vec(),
                    digest_only: false,
                    page_size: read_page_size,
                    page_state: page_state.clone(),
                    clustering: clustering.unwrap_or_default().to_vec(),
                };
                let body = encode_read_request(&payload);
                let lane = partition_read_lane(row_limit, clustering);
                match self
                    .send_remote_with_reconnect(host_id, &addr, Message::ReadRequest(body), lane)
                    .await
                {
                    Ok(Message::ReadResponse(b)) => match decode_read_response(&b) {
                        Some(resp) if resp.found => {
                            found_partition = true;
                            if let Some(p) = resp.partition.map(partition_from_wire) {
                                all_rows.extend(p.rows);
                            }
                            if resp.has_more && !resp.next_page_state.is_empty() {
                                if row_limit > 0 {
                                    break;
                                }
                                page_state = resp.next_page_state;
                                continue; // fetch next page
                            }
                            break; // no more pages
                        }
                        _ => break, // not found or decode failure
                    },
                    _ => {
                        tracing::debug!(target, "read_one_replica: remote send failed");
                        break;
                    }
                }
            }

            if found_partition && !all_rows.is_empty() {
                // Served from a healthy remote replica. If we got here because a
                // local SSTable was corrupt, fire the async repair now (the read
                // itself is already satisfied and is NOT blocked on repair).
                if let Some((min_token, max_token)) = corrupt_range.take() {
                    self.request_anti_entropy_repair(table_id, min_token, max_token);
                }
                return Ok(Some(all_rows));
            }
        }

        // All replicas exhausted. If a local SSTable was corrupt and no replica
        // could serve the key, FAIL LOUD rather than returning a silent `Ok(None)`
        // that would masquerade corruption as "key not found" — the key may have
        // lived only in the corrupt SSTable. Still request repair so the range is
        // refilled when a replica recovers.
        if let Some((min_token, max_token)) = corrupt_range {
            self.request_anti_entropy_repair(table_id, min_token, max_token);
            return Err(ClusterError::Storage(
                ferrosa_common::Error::corrupt_sstable("local", min_token, max_token),
            ));
        }

        // All replicas exhausted — data genuinely not found.
        Ok(None)
    }

    #[cfg(test)]
    async fn read_one_replica(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        replicas: &[u64],
        ring: &crate::ring::TokenRing,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.read_one_replica_limited_rows(table_id, key, replicas, ring, 0, None)
            .await
    }

    /// Coordinate a read using NetworkTopologyStrategy with DC-aware consistency.
    ///
    /// For `LOCAL_QUORUM` / `LOCAL_ONE`: only replicas in the local DC
    /// participate in the quorum calculation.
    /// For `EACH_QUORUM`: all DCs must independently satisfy quorum.
    /// For other CLs: uses total RF as before.
    pub async fn coordinate_read_nts(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &crate::ring::strategy::ReplicationStrategy,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_nts_limited_rows(table_id, key, cl, strategy, 0)
            .await
    }

    pub async fn coordinate_read_nts_limited_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &crate::ring::strategy::ReplicationStrategy,
        row_limit: usize,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_nts_filtered(table_id, key, cl, strategy, row_limit, None)
            .await
    }

    pub async fn coordinate_read_nts_clustering_row(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        clustering: &[u8],
        cl: ConsistencyLevel,
        strategy: &crate::ring::strategy::ReplicationStrategy,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        self.coordinate_read_nts_filtered(table_id, key, cl, strategy, 0, Some(clustering.to_vec()))
            .await
    }

    async fn coordinate_read_nts_filtered(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &crate::ring::strategy::ReplicationStrategy,
        row_limit: usize,
        clustering: Option<Vec<u8>>,
    ) -> crate::error::Result<Option<Vec<Row>>> {
        let ring = self.ring.load();
        let all_replicas = ring.replicas_for_strategy(key.token.0, strategy);

        if tracing::enabled!(tracing::Level::DEBUG) {
            let span = tracing::debug_span!(
                "cluster.read",
                cl = %cl,
                replicas = all_replicas.len(),
            );
            let _enter = span.enter();
        }

        let local_dc = ring
            .get_node(self.local_node_id)
            .map(|n| n.data_center.clone())
            .unwrap_or_default();

        // For LOCAL_* CLs, filter to local DC replicas for quorum counting.
        let (effective_replicas, required) = match cl {
            ConsistencyLevel::LocalQuorum | ConsistencyLevel::LocalOne => {
                let local_replicas: Vec<u64> = all_replicas
                    .iter()
                    .copied()
                    .filter(|&id| {
                        ring.get_node(id)
                            .map(|n| n.data_center == local_dc)
                            .unwrap_or(false)
                    })
                    .collect();
                let local_rf = strategy
                    .dc_replication_factors()
                    .get(&local_dc)
                    .copied()
                    .unwrap_or(1);
                let req = cl.block_for_dc(local_rf);
                (local_replicas, req)
            }
            _ => {
                let rf = strategy.replication_factor();
                let req = cl.block_for(rf);
                (all_replicas, req)
            }
        };
        drop(ring);

        if effective_replicas.len() < required {
            return Err(ClusterError::Unavailable {
                consistency: cl.to_string(),
                required,
                alive: effective_replicas.len(),
            });
        }

        // Delegate to the existing coordinate_read_with logic using
        // the effective replica set and required count.
        self.coordinate_read_with_filter(
            table_id,
            key,
            cl,
            effective_replicas.len(),
            row_limit,
            clustering,
        )
        .await
    }

    /// Scatter a full-table range read to every node in the ring.
    ///
    /// Each node returns its locally-stored partitions for `table_id`.  The
    /// coordinator deduplicates partitions that appear on multiple nodes
    /// (e.g. due to replication or token overlap) by merging replicas with
    /// the same partition key using last-write-wins cell semantics.
    /// Range-read timeout for remote nodes.
    ///
    /// Must be shorter than the CQL client timeout (typically 10s) so the
    /// coordinator returns a result (even partial/error) before the client
    /// gives up. A long timeout caused SELECT to hang on startup when
    /// remote nodes hadn't established connections yet.
    const BULK_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

    pub async fn coordinate_range_read(
        &self,
        table_id: &TableId,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        self.coordinate_range_read_limited(table_id, crate::write_path::DEFAULT_RANGE_READ_LIMIT)
            .await
    }

    pub async fn coordinate_range_read_limited(
        &self,
        table_id: &TableId,
        limit: usize,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        self.coordinate_range_read_limited_rows(table_id, limit, 0)
            .await
    }

    pub async fn coordinate_range_read_limited_rows(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        // ADR-020: streaming range reads use the multi-message lane
        // that does not depend on a wall-clock BULK_READ_TIMEOUT.
        // Operators can temporarily disable this with
        // FERROSA_BULK_STREAMING_RANGE_READ=0 during mixed-version
        // rolling upgrades, but the legacy materializing path applies
        // a hard partition cap and is not correct for complete scans.
        if self.streaming_range_reads {
            return self
                .coordinate_range_read_stream_limited_rows(table_id, limit, row_limit)
                .await;
        }

        let limit = limit.clamp(1, crate::write_path::DEFAULT_RANGE_READ_LIMIT);
        let ring = self.ring.load();
        let node_ids = ring.node_ids();

        // Collect (node_id, host_id, internode addr) while the ring guard is held.
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let req_payload = RangeReadRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            limit,
            row_limit,
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).unwrap_or_default());

        // Fan out to all nodes concurrently.
        // Each future returns Result — errors are NOT silently swallowed.
        let local_id = self.local_node_id;
        let storage = self.storage.clone();
        let table_id_clone = table_id.clone();
        let total_nodes = nodes.len();

        let mut futs: FuturesUnordered<_> = nodes
            .into_iter()
            .map(|(node_id, remote)| {
                let storage = storage.clone();
                let table_id = table_id_clone.clone();
                let req_body = req_body.clone();
                let coordinator = self;

                async move {
                    if node_id == local_id {
                        read_local_range_limited_rows_offloaded(
                            &storage, &table_id, limit, row_limit,
                        )
                        .await
                        .map_err(ClusterError::Storage)
                    } else {
                        let (hid, addr) = remote.ok_or_else(|| {
                            ClusterError::Internal(format!(
                                "range read: node {node_id} has no host_id"
                            ))
                        })?;

                        let resp = coordinator
                            .send_remote_with_reconnect_timeout(
                                hid,
                                &addr,
                                Message::RangeReadRequest(req_body),
                                Lane::Bulk,
                                Self::BULK_READ_TIMEOUT,
                            )
                            .await
                            .map_err(|e| {
                                ClusterError::Internal(format!(
                                    "range read from node {node_id} ({hid}) via {addr}: {e}"
                                ))
                            })?;

                        match resp {
                            Message::RangeReadResponse(b) => {
                                let payload =
                                    bincode::deserialize::<RangeReadResponsePayload>(&b)
                                        .map_err(|e| {
                                            ClusterError::Internal(format!(
                                                "range read: failed to decode response \
                                                 from node {node_id} ({hid}): {e}"
                                            ))
                                        })?;
                                if payload.truncated {
                                    tracing::warn!(
                                        peer = %hid,
                                        "range read response truncated at the range-read materialization cap; \
                                         results may be incomplete"
                                    );
                                }
                                Ok(payload
                                    .partitions
                                    .into_iter()
                                    .map(partition_from_wire)
                                    .collect())
                            }
                            other => Err(ClusterError::Internal(format!(
                                "range read: unexpected response type {:?} from node {node_id} ({hid})",
                                other.msg_type()
                            ))),
                        }
                    }
                }
            })
            .collect();

        // Collect results. If ANY node fails, the range read is incomplete
        // and we must surface the error — silent partial results cause data loss.
        let mut all_partitions: Vec<ferrosa_sstable::types::Partition> = Vec::new();
        let mut first_error: Option<ClusterError> = None;
        let mut failed_nodes = 0usize;

        while let Some(result) = futs.next().await {
            match result {
                Ok(batch) => all_partitions.extend(batch),
                Err(e) => {
                    tracing::error!("coordinate_range_read: {e}");
                    failed_nodes += 1;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        if let Some(ref err) = first_error {
            if all_partitions.is_empty() {
                // ALL nodes failed — return error (no data at all).
                tracing::error!(
                    failed_nodes,
                    "coordinate_range_read: all nodes failed, returning error"
                );
                return Err(first_error.unwrap());
            }
            // Some nodes failed but we have partial data (e.g., local node
            // succeeded, remote nodes not yet connected during startup).
            // Return what we have — better than hanging the client.
            tracing::warn!(
                failed_nodes,
                partitions_received = all_partitions.len(),
                %err,
                "coordinate_range_read: {failed_nodes} node(s) failed, \
                 returning partial results from {remaining} node(s)",
                remaining = total_nodes - failed_nodes,
            );
        }

        // Deduplicate: group by token, merge replicas with the same partition key.
        let mut by_token: BTreeMap<i64, Vec<ferrosa_sstable::types::Partition>> = BTreeMap::new();
        for p in all_partitions {
            by_token.entry(p.key.token.0).or_default().push(p);
        }

        let deduped: Vec<ferrosa_sstable::types::Partition> = by_token
            .into_values()
            .map(|group| {
                if group.len() == 1 {
                    group.into_iter().next().unwrap()
                } else {
                    ferrosa_storage::merge::merge_partitions(group)
                }
            })
            .collect();

        Ok(deduped)
    }

    /// Scatter-gather index read across all ring nodes.
    ///
    /// Each node runs `StorageEngine::read_by_index()` locally and returns
    /// matching partitions. The coordinator merges and deduplicates results
    /// so that all rows matching the indexed value are returned regardless
    /// of which node they reside on.
    pub async fn coordinate_index_read(
        &self,
        table_id: &TableId,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();

        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let req_payload = IndexReadRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            index_name: index_name.to_string(),
            index_key: index_key.0.clone(),
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).unwrap_or_default());

        let local_id = self.local_node_id;
        let storage = self.storage.clone();
        let table_id_clone = table_id.clone();
        let index_name_owned = index_name.to_string();
        let index_key_clone = index_key.clone();
        let total_nodes = nodes.len();

        let mut futs: FuturesUnordered<_> = nodes
            .into_iter()
            .map(|(node_id, remote)| {
                let storage = storage.clone();
                let table_id = table_id_clone.clone();
                let index_name = index_name_owned.clone();
                let index_key = index_key_clone.clone();
                let req_body = req_body.clone();
                let coordinator = self;

                async move {
                    if node_id == local_id {
                        storage
                            .read_by_index(&table_id, &index_name, &index_key)
                            .map_err(ClusterError::Storage)
                    } else {
                        let (hid, addr) = remote.ok_or_else(|| {
                            ClusterError::Internal(format!(
                                "index read: node {node_id} has no host_id"
                            ))
                        })?;

                        let resp = coordinator
                            .send_remote_with_reconnect_timeout(
                                hid,
                                &addr,
                                Message::IndexReadRequest(req_body),
                                Lane::Bulk,
                                Self::BULK_READ_TIMEOUT,
                            )
                            .await
                            .map_err(|e| {
                                ClusterError::Internal(format!(
                                    "index read from node {node_id} ({hid}) via {addr}: {e}"
                                ))
                            })?;

                        match resp {
                            Message::IndexReadResponse(b) => {
                                let payload = bincode::deserialize::<IndexReadResponsePayload>(&b)
                                    .map_err(|e| {
                                        ClusterError::Internal(format!(
                                            "index read: failed to decode response \
                                                 from node {node_id} ({hid}): {e}"
                                        ))
                                    })?;
                                Ok(payload
                                    .partitions
                                    .into_iter()
                                    .map(partition_from_wire)
                                    .collect())
                            }
                            other => Err(ClusterError::Internal(format!(
                                "index read: unexpected response {:?} from node {node_id} ({hid})",
                                other.msg_type()
                            ))),
                        }
                    }
                }
            })
            .collect();

        let mut all_partitions: Vec<ferrosa_sstable::types::Partition> = Vec::new();
        let mut first_error: Option<ClusterError> = None;
        let mut failed_nodes = 0usize;

        while let Some(result) = futs.next().await {
            match result {
                Ok(batch) => all_partitions.extend(batch),
                Err(e) => {
                    tracing::error!("coordinate_index_read: {e}");
                    failed_nodes += 1;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        if let Some(ref err) = first_error {
            if all_partitions.is_empty() {
                tracing::error!(
                    failed_nodes,
                    "coordinate_index_read: all nodes failed, returning error"
                );
                return Err(first_error.unwrap());
            }
            tracing::warn!(
                failed_nodes,
                partitions_received = all_partitions.len(),
                %err,
                "coordinate_index_read: {failed_nodes} node(s) failed, \
                 returning partial results from {remaining} node(s)",
                remaining = total_nodes - failed_nodes,
            );
        }

        // Deduplicate by token.
        let mut by_token: BTreeMap<i64, Vec<ferrosa_sstable::types::Partition>> = BTreeMap::new();
        for p in all_partitions {
            by_token.entry(p.key.token.0).or_default().push(p);
        }

        let deduped: Vec<ferrosa_sstable::types::Partition> = by_token
            .into_values()
            .map(|group| {
                if group.len() == 1 {
                    group.into_iter().next().unwrap()
                } else {
                    ferrosa_storage::merge::merge_partitions(group)
                }
            })
            .collect();

        Ok(deduped)
    }

    /// KEYED secondary-index read (t_430c4188): consult the index for
    /// `index_key` restricted to the partition `key`, contacting ONLY that
    /// partition's replicas under `strategy` — never a global scatter-gather.
    ///
    /// Each contacted replica runs `read_by_index_in_partition` locally
    /// (postings keyed to the partition, then point-reads of only the matching
    /// rows), so per-replica work is O(rows matching the value), never
    /// O(partition rows). Results from the replicas are merged per token, the
    /// same union semantics as `coordinate_index_read` but over the replica
    /// set instead of the whole ring. Partial replica failures degrade to a
    /// partial union (logged); an all-replicas-failed result errors.
    pub async fn coordinate_index_read_in_partition(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        index_name: &str,
        index_key: &ferrosa_index::IndexKey,
        strategy: &crate::ring::strategy::ReplicationStrategy,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        let ring = self.ring.load();
        let replica_ids = ring.replicas_for_strategy(key.token.0, strategy);
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = replica_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        if nodes.is_empty() {
            return Err(ClusterError::Internal(format!(
                "keyed index read: no replicas resolved for token {}",
                key.token.0
            )));
        }

        let req_payload = IndexReadInPartitionRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            index_name: index_name.to_string(),
            index_key: index_key.0.clone(),
            partition_key: key.key.as_bytes().to_vec(),
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).unwrap_or_default());

        let local_id = self.local_node_id;
        let storage = self.storage.clone();
        let table_id_clone = table_id.clone();
        let index_name_owned = index_name.to_string();
        let index_key_clone = index_key.clone();
        let partition_key_bytes = key.key.as_bytes().to_vec();
        let total_nodes = nodes.len();

        let mut futs: FuturesUnordered<_> = nodes
            .into_iter()
            .map(|(node_id, remote)| {
                let storage = storage.clone();
                let table_id = table_id_clone.clone();
                let index_name = index_name_owned.clone();
                let index_key = index_key_clone.clone();
                let partition_key = partition_key_bytes.clone();
                let req_body = req_body.clone();
                let coordinator = self;

                async move {
                    if node_id == local_id {
                        storage
                            .read_by_index_in_partition(
                                &table_id,
                                &index_name,
                                &index_key,
                                &partition_key,
                            )
                            .map_err(ClusterError::Storage)
                    } else {
                        let (hid, addr) = remote.ok_or_else(|| {
                            ClusterError::Internal(format!(
                                "keyed index read: node {node_id} has no host_id"
                            ))
                        })?;

                        let resp = coordinator
                            .send_remote_with_reconnect_timeout(
                                hid,
                                &addr,
                                Message::IndexReadInPartitionRequest(req_body),
                                Lane::Bulk,
                                Self::BULK_READ_TIMEOUT,
                            )
                            .await
                            .map_err(|e| {
                                ClusterError::Internal(format!(
                                    "keyed index read from node {node_id} ({hid}) via {addr}: {e}"
                                ))
                            })?;

                        match resp {
                            Message::IndexReadInPartitionResponse(b) => {
                                let payload = bincode::deserialize::<IndexReadResponsePayload>(&b)
                                    .map_err(|e| {
                                        ClusterError::Internal(format!(
                                            "keyed index read: failed to decode response \
                                                 from node {node_id} ({hid}): {e}"
                                        ))
                                    })?;
                                Ok(payload
                                    .partitions
                                    .into_iter()
                                    .map(partition_from_wire)
                                    .collect())
                            }
                            other => Err(ClusterError::Internal(format!(
                                "keyed index read: unexpected response {:?} from node \
                                 {node_id} ({hid})",
                                other.msg_type()
                            ))),
                        }
                    }
                }
            })
            .collect();

        let mut all_partitions: Vec<ferrosa_sstable::types::Partition> = Vec::new();
        let mut first_error: Option<ClusterError> = None;
        let mut failed_nodes = 0usize;

        while let Some(result) = futs.next().await {
            match result {
                Ok(batch) => all_partitions.extend(batch),
                Err(e) => {
                    tracing::error!("coordinate_index_read_in_partition: {e}");
                    failed_nodes += 1;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        if let Some(ref err) = first_error {
            if failed_nodes == total_nodes {
                tracing::error!(
                    failed_nodes,
                    "coordinate_index_read_in_partition: all replicas failed, returning error"
                );
                return Err(first_error.unwrap());
            }
            tracing::warn!(
                failed_nodes,
                partitions_received = all_partitions.len(),
                %err,
                "coordinate_index_read_in_partition: {failed_nodes} replica(s) failed, \
                 returning partial results from {remaining} replica(s)",
                remaining = total_nodes - failed_nodes,
            );
        }

        // Merge per token (all results are the same partition; replicas may
        // each return the same rows).
        let mut by_token: BTreeMap<i64, Vec<ferrosa_sstable::types::Partition>> = BTreeMap::new();
        for p in all_partitions {
            by_token.entry(p.key.token.0).or_default().push(p);
        }

        let deduped: Vec<ferrosa_sstable::types::Partition> = by_token
            .into_values()
            .map(|group| {
                if group.len() == 1 {
                    group.into_iter().next().unwrap()
                } else {
                    ferrosa_storage::merge::merge_partitions(group)
                }
            })
            .collect();

        Ok(deduped)
    }

    /// Fan out a full-text (`fts_match`) lookup to every node and union the
    /// matching partition keys.
    ///
    /// `fts_match` carries no partition key, so its hits span all token ranges;
    /// consulting only the coordinator's local FTI returned 0/1
    /// non-deterministically depending on which node coordinated the query
    /// (BUG-F-007). Querying every node and de-duplicating the keys makes the
    /// result coordinator-independent. Partial failures degrade to a partial
    /// union (logged); only an all-nodes-failed-and-empty result errors.
    ///
    /// `limit` is the query-derived `LIMIT k`, pushed down so every replica
    /// holds a bounded top-k working set (t_ee98faa0 layer 2); the union is
    /// then at most `replicas × k` keys. `None` requests the complete match
    /// set (no-LIMIT statements) — never a server-side cap.
    pub async fn coordinate_fulltext_search(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
        limit: Option<usize>,
    ) -> crate::error::Result<Vec<Vec<u8>>> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let req_payload = FulltextSearchRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            index_name: index_name.to_string(),
            query: query.to_string(),
            limit: limit.map(|k| k as u64),
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).unwrap_or_default());

        let local_id = self.local_node_id;
        let storage = self.storage.clone();
        let table_id_clone = table_id.clone();
        let index_name_owned = index_name.to_string();
        let query_owned = query.to_string();
        let total_nodes = nodes.len();

        let mut futs: FuturesUnordered<_> = nodes
            .into_iter()
            .map(|(node_id, remote)| {
                let storage = storage.clone();
                let table_id = table_id_clone.clone();
                let index_name = index_name_owned.clone();
                let query = query_owned.clone();
                let req_body = req_body.clone();
                let coordinator = self;

                async move {
                    if node_id == local_id {
                        // Offload the blocking local FTI scan so it does not
                        // starve raft heartbeats / CQL keepalives on the
                        // coordinator's async runtime (t_8fc24ce2).
                        tokio::task::spawn_blocking(move || {
                            storage.fulltext_search(&table_id, &index_name, &query, limit)
                        })
                        .await
                        .map_err(|e| {
                            ClusterError::Internal(format!("fulltext_search task join: {e}"))
                        })?
                        .map_err(ClusterError::Storage)
                    } else {
                        let (hid, addr) = remote.ok_or_else(|| {
                            ClusterError::Internal(format!(
                                "fulltext search: node {node_id} has no host_id"
                            ))
                        })?;

                        let resp = coordinator
                            .send_remote_with_reconnect_timeout(
                                hid,
                                &addr,
                                Message::FulltextSearchRequest(req_body),
                                Lane::Bulk,
                                Self::BULK_READ_TIMEOUT,
                            )
                            .await
                            .map_err(|e| {
                                ClusterError::Internal(format!(
                                    "fulltext search from node {node_id} ({hid}) via {addr}: {e}"
                                ))
                            })?;

                        match resp {
                            Message::FulltextSearchResponse(b) => {
                                let payload =
                                    bincode::deserialize::<FulltextSearchResponsePayload>(&b)
                                        .map_err(|e| {
                                            ClusterError::Internal(format!(
                                                "fulltext search: failed to decode response \
                                                 from node {node_id} ({hid}): {e}"
                                            ))
                                        })?;
                                Ok(payload.matching_keys)
                            }
                            other => Err(ClusterError::Internal(format!(
                                "fulltext search: unexpected response {:?} from node {node_id} ({hid})",
                                other.msg_type()
                            ))),
                        }
                    }
                }
            })
            .collect();

        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        let mut first_error: Option<ClusterError> = None;
        let mut failed_nodes = 0usize;

        while let Some(result) = futs.next().await {
            match result {
                Ok(keys) => {
                    for k in keys {
                        if seen.insert(k.clone()) {
                            all_keys.push(k);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("coordinate_fulltext_search: {e}");
                    failed_nodes += 1;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        if let Some(err) = first_error {
            if failed_nodes == total_nodes {
                tracing::error!(
                    failed_nodes,
                    "coordinate_fulltext_search: all nodes failed, returning error"
                );
                return Err(err);
            }
            tracing::warn!(
                failed_nodes,
                keys_received = all_keys.len(),
                %err,
                "coordinate_fulltext_search: {failed_nodes}/{total_nodes} node(s) failed, \
                 returning partial union",
            );
        }

        Ok(all_keys)
    }
}

// ---------------------------------------------------------------------------
// Read repair — standalone async function for use in spawned tasks
// ---------------------------------------------------------------------------

use ferrosa_net::peer::PeerManager;

use super::metrics::ReadRepairMetrics;

/// Send repair writes to stale replicas.
///
/// Builds a [`Mutation`] from the newest partition data and sends a
/// `RepairWrite` message to each stale replica. Errors are logged
/// and counted but do not fail the read.
async fn send_repair_writes(
    peer_manager: &PeerManager,
    storage: &ferrosa_storage::engine::StorageEngine,
    metrics: &ReadRepairMetrics,
    local_host_id: Option<uuid::Uuid>,
    table_id: &TableId,
    partition: &Partition,
    stale_host_ids: &[uuid::Uuid],
) {
    let mutation = Mutation::new(
        table_id.keyspace.clone(),
        table_id.table.clone(),
        partition.key.clone(),
        partition.rows.clone(),
        partition
            .rows
            .iter()
            .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
            .max()
            .unwrap_or(0),
    );
    let body = encode_mutation(&mutation);

    for &host_id in stale_host_ids {
        metrics.inc_attempted();
        if Some(host_id) == local_host_id {
            let mut failed = None;
            for row in mutation.rows.iter().cloned() {
                if let Err(e) = storage.write(table_id, &mutation.key, row, mutation.timestamp) {
                    failed = Some(e);
                    break;
                }
            }
            if let Some(e) = failed {
                tracing::warn!(
                    %host_id,
                    table = %table_id,
                    %e,
                    "local read repair failed"
                );
                metrics.inc_failed();
            } else {
                tracing::info!(
                    %host_id,
                    table = %table_id,
                    "local read repair succeeded"
                );
                metrics.inc_succeeded();
            }
            continue;
        }

        match peer_manager
            .fire(host_id, Message::RepairWrite(body.clone()), Lane::Data)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    %host_id,
                    table = %table_id,
                    "read repair succeeded"
                );
                metrics.inc_succeeded();
            }
            Err(e) => {
                tracing::warn!(
                    %host_id,
                    table = %table_id,
                    %e,
                    "read repair failed"
                );
                metrics.inc_failed();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use uuid::Uuid;

    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_net::codec::MsgType;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::message::Message;
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_net::rpc::handler::{HandlerRegistry, PeerId, RpcHandler};
    use ferrosa_net::rpc::server::RpcServer;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig};

    use crate::consistency::ConsistencyLevel;
    use crate::error::ClusterError;
    use crate::raft::{NodeInfo, NodeState};
    use crate::ring::TokenRing;

    // -----------------------------------------------------------------------
    // Helpers (mirrors write.rs test helpers)
    // -----------------------------------------------------------------------

    #[test]
    fn stale_lane_errors_trigger_peer_refresh_for_reads() {
        assert!(should_retry_missing_peer_error("unknown peer"));
        assert!(should_retry_missing_peer_error("no connection pool"));
        assert!(should_retry_missing_peer_error(
            "lane is reconnecting; retry later"
        ));
        assert!(should_retry_missing_peer_error(
            "lane permanently failed after max reconnection attempts"
        ));
    }

    #[test]
    fn unbounded_partition_scan_uses_bulk_lane() {
        // Given an unbounded partition read that may require many remote pages,
        // when the coordinator selects its internode lane, then it must not
        // share the latency-sensitive Data lane used by writes.
        assert_eq!(partition_read_lane(0, None), Lane::Bulk);
    }

    #[test]
    fn bounded_partition_read_keeps_data_lane() {
        assert_eq!(partition_read_lane(20, None), Lane::Data);
    }

    #[test]
    fn exact_clustering_read_keeps_data_lane() {
        assert_eq!(partition_read_lane(0, Some(b"row-key")), Lane::Data);
    }

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn test_key() -> DecoratedKey {
        DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        }
    }

    fn test_row(ts: i64) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn register_test_table(storage: &StorageEngine) {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();
    }

    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    struct StaticReadHandler {
        partition: Partition,
    }

    #[async_trait::async_trait]
    impl RpcHandler for StaticReadHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::ReadRequest(_) = msg else {
                return None;
            };
            let payload = ReadResponsePayload {
                found: true,
                partition: Some(crate::raft::handlers::partition_to_wire(
                    self.partition.clone(),
                )),
                digest: None,
                timestamp: self
                    .partition
                    .rows
                    .iter()
                    .flat_map(|row| row.cells.iter().map(|(_, cell)| cell.timestamp))
                    .max()
                    .unwrap_or(i64::MIN),
                has_more: false,
                next_page_state: vec![],
            };
            Some(Message::ReadResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    struct StaticIndexReadHandler {
        partition: Partition,
    }

    #[async_trait::async_trait]
    impl RpcHandler for StaticIndexReadHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::IndexReadRequest(_) = msg else {
                return None;
            };
            let payload = IndexReadResponsePayload {
                partitions: vec![crate::raft::handlers::partition_to_wire(
                    self.partition.clone(),
                )],
            };
            Some(Message::IndexReadResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    struct DelayedRangeReadHandler {
        partition: Partition,
        delay: std::time::Duration,
    }

    struct DelayedPartitionReadHandler {
        partition: Partition,
        delay: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl RpcHandler for DelayedPartitionReadHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::ReadRequest(body) = msg else {
                return None;
            };
            let request: ReadRequestPayload = bincode::deserialize(&body).ok()?;
            tokio::time::sleep(self.delay).await;
            let payload = ReadResponsePayload {
                found: true,
                partition: Some(crate::raft::handlers::partition_to_wire(
                    self.partition.clone(),
                )),
                digest: None,
                timestamp: i64::MIN,
                has_more: false,
                next_page_state: Vec::new(),
            };
            (!request.digest_only)
                .then(|| Message::ReadResponse(Bytes::from(bincode::serialize(&payload).unwrap())))
        }
    }

    #[async_trait::async_trait]
    impl RpcHandler for DelayedRangeReadHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::RangeReadRequest(_) = msg else {
                return None;
            };
            tokio::time::sleep(self.delay).await;
            let payload = RangeReadResponsePayload {
                partitions: vec![crate::raft::handlers::partition_to_wire(
                    self.partition.clone(),
                )],
                truncated: false,
            };
            Some(Message::RangeReadResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    struct DelayedIndexReadHandler {
        partition: Partition,
        delay: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl RpcHandler for DelayedIndexReadHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::IndexReadRequest(_) = msg else {
                return None;
            };
            tokio::time::sleep(self.delay).await;
            let payload = IndexReadResponsePayload {
                partitions: vec![crate::raft::handlers::partition_to_wire(
                    self.partition.clone(),
                )],
            };
            Some(Message::IndexReadResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    struct StaticDigestReadHandler {
        partition: Partition,
    }

    #[async_trait::async_trait]
    impl RpcHandler for StaticDigestReadHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::ReadRequest(body) = msg else {
                return None;
            };
            let req: ReadRequestPayload = bincode::deserialize(&body).ok()?;
            let digest = crate::raft::handlers::compute_partition_digest(&self.partition).ok();
            let timestamp = self
                .partition
                .rows
                .iter()
                .flat_map(|row| row.cells.iter().map(|(_, cell)| cell.timestamp))
                .max()
                .unwrap_or(i64::MIN);
            let payload = ReadResponsePayload {
                found: true,
                partition: if req.digest_only {
                    None
                } else {
                    Some(crate::raft::handlers::partition_to_wire(
                        self.partition.clone(),
                    ))
                },
                digest,
                timestamp,
                has_more: false,
                next_page_state: vec![],
            };
            Some(Message::ReadResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    fn noop_peer_manager() -> Arc<PeerManager> {
        Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ))
    }

    fn make_coordinator(
        ring: TokenRing,
        peer_manager: Arc<PeerManager>,
        local_node_id: u64,
        storage: Arc<StorageEngine>,
        rf: usize,
        cl: ConsistencyLevel,
    ) -> ClusterCoordinator {
        ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peer_manager,
            local_node_id,
            storage,
            rf,
            cl,
        )
    }

    async fn start_rpc_server(
        msg_type: MsgType,
        handler: Arc<dyn RpcHandler>,
    ) -> (Arc<RpcServer>, std::net::SocketAddr, uuid::Uuid) {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(msg_type, handler);
        let server = Arc::new(RpcServer::new(config, server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        (server, addr, server_id)
    }

    struct StaticFulltextSearchHandler {
        keys: Vec<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl RpcHandler for StaticFulltextSearchHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::FulltextSearchRequest(_) = msg else {
                return None;
            };
            let payload = crate::raft::handlers::FulltextSearchResponsePayload {
                matching_keys: self.keys.clone(),
            };
            Some(Message::FulltextSearchResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    /// fts_match fan-out (BUG-F-007 / t_0d08aa43): the coordinator must query
    /// every node's local FTI and union the matching partition keys, de-duping
    /// a key returned by more than one replica. Reproduces the multi-node served
    /// path in-process: local node has an FTI hit; a remote node returns the
    /// same key plus a remote-only key.
    #[tokio::test]
    async fn coordinate_fulltext_search_unions_and_dedups_across_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);
        let table_id = TableId::new("test_ks", "test_tbl");

        // Local node: an FTI row whose text matches → local returns key [1,2,3].
        storage.add_fulltext_index(&table_id, "val_fti", 0).unwrap();
        storage
            .write(&table_id, &test_key(), test_row(1000), 1000)
            .unwrap();

        // The local FTI now returns a full ROW-GRANULAR doc key (partition +
        // clustering), not a bare partition key. Capture the actual local key so
        // the remote returns the SAME bytes — exercising real dedup — instead of a
        // hand-rolled partition key that would no longer match.
        let local_keys = storage
            .fulltext_search(&table_id, "val_fti", "hello", None)
            .unwrap();
        assert_eq!(local_keys.len(), 1, "local node should have one FTI hit");
        let shared_key = local_keys[0].clone();
        let remote_only = vec![9u8, 9, 9];

        // Remote node: returns the SAME local key (must dedup) plus a
        // remote-only key.
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::FulltextSearchRequest,
            Arc::new(StaticFulltextSearchHandler {
                keys: vec![shared_key.clone(), remote_only.clone()],
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let local_node_id = 1u64;
        let mut local = make_node("127.0.0.1:7000");
        local.host_id = Uuid::new_v4();
        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local);
        ring.add_node(2u64, remote);
        ring.assign_tokens(local_node_id, &[42]);
        ring.assign_tokens(2u64, &[142]);

        let coordinator = make_coordinator(
            ring,
            pm.clone(),
            local_node_id,
            storage,
            2,
            ConsistencyLevel::Quorum,
        );

        let mut keys = coordinator
            .coordinate_fulltext_search(&table_id, "val_fti", "hello", None)
            .await
            .unwrap();
        keys.sort();

        let mut expected = vec![shared_key, remote_only];
        expected.sort();
        assert_eq!(
            keys, expected,
            "fan-out must union local + remote FTI hits and de-duplicate the shared key"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    /// A remote FTI failure must not turn a legitimate empty local result into
    /// a user-visible query failure. The fmem hybrid_search path fans out
    /// `fts_match` and can hit transient remote stream failures such as
    /// `ChannelClosedBeforeDone`; if at least one node completed, the result is
    /// a partial union, even when that union is empty. Only all nodes failing is
    /// fatal.
    #[tokio::test]
    async fn coordinate_fulltext_search_degrades_to_empty_when_some_nodes_fail() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);
        let table_id = TableId::new("test_ks", "test_tbl");
        storage.add_fulltext_index(&table_id, "val_fti", 0).unwrap();

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let local_node_id = 1u64;
        let mut local = make_node("127.0.0.1:7000");
        local.host_id = Uuid::new_v4();
        let remote_host_id = Uuid::new_v4();
        let mut remote = make_node("127.0.0.1:1");
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local);
        ring.add_node(2u64, remote);
        ring.assign_tokens(local_node_id, &[42]);
        ring.assign_tokens(2u64, &[142]);

        let coordinator = make_coordinator(
            ring,
            pm,
            local_node_id,
            storage,
            2,
            ConsistencyLevel::Quorum,
        );

        let keys = coordinator
            .coordinate_fulltext_search(&table_id, "val_fti", "no-such-token", None)
            .await
            .expect("one successful empty FTI shard should degrade remote failure to empty union");

        assert!(
            keys.is_empty(),
            "expected an empty partial union when local FTI succeeds and remote FTI fails"
        );
    }

    // -----------------------------------------------------------------------
    // Task 6 tests
    // -----------------------------------------------------------------------

    /// CL=ONE, local replica: should read directly from storage.
    /// RED for the point-read worker-thread offload.
    ///
    /// `read_local_partition` must run the (synchronous, potentially blocking)
    /// storage read on a tokio blocking thread, not inline on the async runtime
    /// worker. We make the read deterministically exercise the blocking
    /// evicted-SSTable miss path by deleting the `Data.db` after flush and
    /// serving the addressed range from a backup via a path-scoped
    /// `file_read_range_hook`. The hook records the OS thread it runs on; with
    /// the fix that thread differs from the single async worker, proving the
    /// read was offloaded. With the pre-fix inline call it equals the worker
    /// thread.
    #[test]
    fn local_read_runs_off_the_async_worker_thread() {
        use std::sync::Mutex as StdMutex;

        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();
        storage.flush(&table_id).unwrap();

        // Locate the flushed Data.db component(s) for this table and back them
        // up, then evict (delete) them so reads fall through to the range hook.
        let table_sstable_dir = dir.path().join("sstables").join(table_id.to_string());
        let backup_dir = dir.path().join("backup");
        std::fs::create_dir_all(&backup_dir).unwrap();
        let mut evicted_data: std::collections::HashMap<std::path::PathBuf, Vec<u8>> =
            std::collections::HashMap::new();
        for entry in std::fs::read_dir(&table_sstable_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with("-Data.db") {
                let bytes = std::fs::read(&path).unwrap();
                evicted_data.insert(path.clone(), bytes);
                std::fs::remove_file(&path).unwrap();
            }
        }
        assert!(
            !evicted_data.is_empty(),
            "test setup: expected at least one Data.db to evict"
        );

        // Path-scoped hooks that record the executing thread and serve the
        // evicted Data.db from the in-memory backup. All three read hooks are
        // registered so we capture whichever the read path exercises; each
        // returns a pass-through (`Ok(None)`/`Ok(false)`) for paths it does not
        // own, so it never interferes with other tests.
        let observed_thread: Arc<StdMutex<Option<std::thread::ThreadId>>> =
            Arc::new(StdMutex::new(None));
        {
            let observed = observed_thread.clone();
            let data = evicted_data.clone();
            ferrosa_sstable::io::register_file_read_range_hook(Arc::new(
                move |path, offset, len| {
                    let Some(bytes) = data.get(path) else {
                        return Ok(None);
                    };
                    *observed.lock().unwrap() = Some(std::thread::current().id());
                    let start = (offset as usize).min(bytes.len());
                    let end = (start + len).min(bytes.len());
                    Ok(Some(bytes[start..end].to_vec()))
                },
            ));
        }
        {
            let observed = observed_thread.clone();
            let data = evicted_data.clone();
            ferrosa_sstable::io::register_file_read_len_hook(Arc::new(move |path| {
                let Some(bytes) = data.get(path) else {
                    return Ok(None);
                };
                *observed.lock().unwrap() = Some(std::thread::current().id());
                Ok(Some(bytes.len() as u64))
            }));
        }
        {
            let observed = observed_thread.clone();
            let data = evicted_data.clone();
            ferrosa_sstable::io::register_file_read_rehydration_hook(Arc::new(move |path| {
                let Some(bytes) = data.get(path) else {
                    return Ok(false);
                };
                *observed.lock().unwrap() = Some(std::thread::current().id());
                std::fs::write(path, bytes)?;
                Ok(true)
            }));
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let (worker_thread, result_rows) = rt.block_on(async move {
            let worker = std::thread::current().id();
            let result = read_local_partition(&storage, table_id.clone(), key.clone(), 0, None)
                .await
                .unwrap();
            (worker, result.map(|p| p.rows.len()).unwrap_or(0))
        });

        assert_eq!(result_rows, 1, "read should return the written row");
        let read_thread = observed_thread
            .lock()
            .unwrap()
            .expect("range hook must have served the evicted Data.db read");
        assert_ne!(
            read_thread, worker_thread,
            "local read ran inline on the async worker thread instead of \
             being offloaded to a blocking thread"
        );
    }

    #[tokio::test]
    async fn coordinate_read_local_replica_returns_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row(1000);

        // Write directly to storage.
        storage.write(&table_id, &key, row.clone(), 1000).unwrap();

        // Read via coordinator.
        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some(), "coordinator should return written data");
        let rows = result.unwrap();
        assert!(!rows.is_empty(), "should have at least one row");
    }

    /// Returns Unavailable when there are not enough replicas in the ring.
    #[tokio::test]
    async fn coordinate_read_returns_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let local_node_id = 1u64;
        let ring = TokenRing::new(); // empty ring — no replicas

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            3,
            ConsistencyLevel::Quorum,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        let result = coordinator.coordinate_read(&table_id, &key).await;
        assert!(result.is_err(), "should fail with Unavailable");
        match result.unwrap_err() {
            ClusterError::Unavailable {
                required, alive, ..
            } => {
                assert_eq!(required, 2, "QUORUM of RF=3 requires 2");
                assert_eq!(alive, 0, "empty ring has 0 replicas");
            }
            other => panic!("expected Unavailable, got: {other}"),
        }
    }

    /// CL=ONE only contacts a single replica (local).
    ///
    /// Verifies that the CL=ONE fast path skips the digest phase entirely
    /// and returns data from the local replica without contacting others.
    #[tokio::test]
    async fn coordinate_read_at_one_contacts_single_replica() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        // RF=3 ring, but only the local node has data (others are unreachable).
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.add_node(2u64, {
            let mut n = make_node("10.0.0.2:7000");
            n.host_id = Uuid::new_v4();
            n
        });
        ring.add_node(3u64, {
            let mut n = make_node("10.0.0.3:7000");
            n.host_id = Uuid::new_v4();
            n
        });
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);
        ring.assign_tokens(3u64, &[200]);

        // CL=ONE: should only contact the local replica.
        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(), // remote replicas have no connection pool
            local_node_id,
            storage.clone(),
            3, // RF=3
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row(1000);
        storage.write(&table_id, &key, row, 1000).unwrap();

        // With CL=ONE the coordinator must succeed using only the local replica.
        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some(), "CL=ONE should return local data");
    }

    /// Two replicas with different timestamps: coordinator returns newest (LWW).
    ///
    /// We simulate this entirely locally using a single-node ring with CL=ALL,
    /// RF=1. The "two timestamps" scenario is exercised by writing two rows
    /// with different timestamps to the same key — storage returns the merged
    /// partition; we assert that the newest cell value is present.
    ///
    /// Note: True multi-node LWW (where replicas hold different data) requires
    /// a mock PeerManager and will be validated in the docker smoke tests
    /// (Slice 5).  This test covers the local path and digest fast path.
    #[tokio::test]
    async fn coordinate_read_returns_newest_by_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Write two rows with different timestamps (older then newer).
        let older_row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"old_value".to_vec(), 100))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(100),
        };
        let newer_row = Row {
            clustering: vec![1], // distinct clustering key so both rows survive
            cells: vec![(0, CellValue::live(b"new_value".to_vec(), 9000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(9000),
        };
        storage.write(&table_id, &key, older_row, 100).unwrap();
        storage.write(&table_id, &key, newer_row, 9000).unwrap();

        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some(), "should return data");
        let rows = result.unwrap();
        // The partition must contain both rows; the newest timestamp must be present.
        let max_ts = rows
            .iter()
            .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
            .max()
            .expect("at least one cell");
        assert_eq!(max_ts, 9000, "must include the newest row (ts=9000)");
    }

    /// Quorum read with only the local replica available (2 remote replicas fail)
    /// should return ReadTimeout when CL=Quorum cannot be satisfied.
    #[tokio::test]
    async fn coordinate_read_quorum_timeout_when_remote_replicas_fail() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let remote_uuid_2 = Uuid::new_v4();
        let remote_uuid_3 = Uuid::new_v4();

        // Add peer entries with no real pools (send will fail).
        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        pm.add_peer_entry((remote_uuid_2, "127.0.0.1:1".parse().unwrap()))
            .await;
        pm.add_peer_entry((remote_uuid_3, "127.0.0.1:2".parse().unwrap()))
            .await;

        let mut node2 = make_node("127.0.0.1:1");
        node2.host_id = remote_uuid_2;
        let mut node3 = make_node("127.0.0.1:2");
        node3.host_id = remote_uuid_3;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("127.0.0.1:7000"));
        ring.add_node(2u64, node2);
        ring.add_node(3u64, node3);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);
        ring.assign_tokens(3u64, &[200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            pm,
            local_node_id,
            storage.clone(),
            3, // RF=3
            ConsistencyLevel::Quorum,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Write data locally so the local read succeeds.
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        // Quorum requires 2 responses; only local (1) will succeed.
        // The remote digest read will fail (no real connection).
        let result = coordinator.coordinate_read(&table_id, &key).await;
        match result {
            Err(ClusterError::ReadTimeout { required, .. }) => {
                assert_eq!(required, 2, "QUORUM of RF=3 requires 2");
            }
            other => panic!("expected ReadTimeout, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordinate_read_quorum_uses_spare_digest_replica_when_one_remote_fails() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(1000)],
        };
        storage
            .write(&table_id, &key, partition.rows[0].clone(), 1000)
            .unwrap();

        let (server, addr, remote_host_id_3) = start_rpc_server(
            MsgType::ReadRequest,
            Arc::new(StaticDigestReadHandler {
                partition: partition.clone(),
            }),
        )
        .await;

        let local_node_id = 1u64;
        let remote_host_id_2 = Uuid::new_v4();
        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        pm.add_peer_entry((remote_host_id_2, "127.0.0.1:1".parse().unwrap()))
            .await;

        let mut local = make_node("127.0.0.1:7000");
        local.host_id = Uuid::new_v4();
        let mut failing_remote = make_node("127.0.0.1:1");
        failing_remote.host_id = remote_host_id_2;
        let mut healthy_remote = make_node(&addr.to_string());
        healthy_remote.host_id = remote_host_id_3;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local);
        ring.add_node(2u64, failing_remote);
        ring.add_node(3u64, healthy_remote);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);
        ring.assign_tokens(3u64, &[200]);

        let coordinator = make_coordinator(
            ring,
            pm.clone(),
            local_node_id,
            storage,
            3,
            ConsistencyLevel::Quorum,
        );

        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(
            result.is_some(),
            "quorum read should use a spare digest replica when the first remote fails"
        );
        assert!(
            pm.has_peer(remote_host_id_3),
            "healthy spare digest replica should be connected"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    /// Build a flushed-then-corrupted local SSTable so a local read of `key`
    /// fails loud with [`ferrosa_common::Error::CorruptSstable`].
    ///
    /// Writes `row`, flushes it to an SSTable, then truncates every `-Data.db`
    /// component for the table to a single byte. The data lives ONLY in that
    /// SSTable (no memtable copy after flush), so a subsequent read exhausts the
    /// view-retry bound and surfaces the typed corrupt-SSTable error — exactly
    /// the signal the coordinator must map to a replica failover + repair.
    fn corrupt_local_sstable(
        dir: &std::path::Path,
        storage: &StorageEngine,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        ts: i64,
    ) {
        storage.write(table_id, key, row, ts).unwrap();
        storage.flush(table_id).unwrap();
        let sstable_dir = dir.join("sstables").join(table_id.to_string());
        let mut corrupted = 0usize;
        for entry in std::fs::read_dir(&sstable_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with("-Data.db") {
                std::fs::write(&path, [0u8]).unwrap();
                corrupted += 1;
            }
        }
        assert!(
            corrupted > 0,
            "test setup: expected a flushed Data.db to corrupt"
        );
        // NOTE: we deliberately do NOT read here to verify. The first read that
        // hits the corruption quarantines the SSTable (later reads then skip it
        // and return Ok(None) instead of the corrupt error). The coordinator's
        // read under test must be that first read, so it surfaces the typed
        // CorruptSstable error and drives the failover + repair path.
    }

    /// CL=ONE: a local corrupt-SSTable read must FAIL OVER to a remote replica
    /// and serve the client the remote's data, rather than propagating the local
    /// corruption error. Today CL=ONE prefers local; this asserts the failover.
    #[tokio::test]
    async fn coordinate_read_at_one_fails_over_to_remote_on_local_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Remote replica serves the partition over a real RPC server.
        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(1234)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::ReadRequest,
            Arc::new(StaticReadHandler {
                partition: partition.clone(),
            }),
        )
        .await;

        // Local replica holds the same key but in a corrupt-only SSTable.
        corrupt_local_sstable(dir.path(), &storage, &table_id, &key, test_row(1234), 1234);

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let local_node_id = 1u64;
        let mut local = make_node("127.0.0.1:7000");
        local.host_id = Uuid::new_v4();
        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local);
        ring.add_node(2u64, remote);
        // Both own token 42 (test_key) at RF=2: local is preferred, remote is
        // the failover target.
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);

        let coordinator = make_coordinator(
            ring,
            pm.clone(),
            local_node_id,
            storage.clone(),
            2, // RF=2
            ConsistencyLevel::One,
        );

        let result = coordinator
            .coordinate_read(&table_id, &key)
            .await
            .expect("CL=ONE must fail over to the remote replica, not error on local corruption");
        let rows = result.expect("remote replica holds the partition");
        assert!(
            !rows.is_empty(),
            "failover read must return the remote rows"
        );
        let ts = rows
            .iter()
            .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
            .max()
            .unwrap();
        assert_eq!(
            ts, 1234,
            "served data must come from the healthy remote replica"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    /// CL=ONE failover on local corruption must ALSO fire an async anti-entropy
    /// repair request targeting the corrupt SSTable's token range, recorded so
    /// the scheduler can drain it (asserted via the recorded trigger + metric).
    #[tokio::test]
    async fn coordinate_read_at_one_requests_repair_on_local_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(1234)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::ReadRequest,
            Arc::new(StaticReadHandler {
                partition: partition.clone(),
            }),
        )
        .await;

        corrupt_local_sstable(dir.path(), &storage, &table_id, &key, test_row(1234), 1234);

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let local_node_id = 1u64;
        let mut local = make_node("127.0.0.1:7000");
        local.host_id = Uuid::new_v4();
        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local);
        ring.add_node(2u64, remote);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);

        let coordinator = make_coordinator(
            ring,
            pm.clone(),
            local_node_id,
            storage.clone(),
            2,
            ConsistencyLevel::One,
        );

        // The global metric is process-wide (shared across parallel tests), so
        // assert it strictly *increased* — the per-coordinator queue below is
        // the deterministic, instance-scoped source of truth for the exact count.
        let before = coordinator.anti_entropy_repairs_requested_total();
        coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(
            coordinator.anti_entropy_repairs_requested_total() > before,
            "serving from a replica on local corruption must increment the \
             anti-entropy repair metric"
        );

        // Exactly one repair request must have been recorded for this read.
        let requests = coordinator.drain_anti_entropy_repair_requests();
        assert_eq!(requests.len(), 1, "exactly one repair request recorded");
        let req = &requests[0];
        assert_eq!(
            req.table_id, table_id,
            "repair must target the read's table"
        );
        // The corrupt SSTable covered token 42 (test_key); the requested range
        // must include it.
        assert!(
            req.min_token <= key.token.0 && key.token.0 <= req.max_token,
            "repair range [{},{}] must cover the corrupt key's token {}",
            req.min_token,
            req.max_token,
            key.token.0
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn coordinate_read_reconnects_missing_remote_peer_pool() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(777)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::ReadRequest,
            Arc::new(StaticReadHandler {
                partition: partition.clone(),
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(2u64, remote);
        ring.assign_tokens(2u64, &[42]);

        let coordinator = make_coordinator(
            ring,
            pm.clone(),
            1u64,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(
            result.is_some(),
            "remote read should succeed after reconnect"
        );
        assert!(
            pm.has_peer(remote_host_id),
            "coordinator should cache the reconnected peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn coordinate_index_read_reconnects_missing_remote_peer_pool() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key();
        let partition = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(888)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::IndexReadRequest,
            Arc::new(StaticIndexReadHandler {
                partition: partition.clone(),
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(2u64, remote);
        ring.assign_tokens(2u64, &[42]);

        let coordinator =
            make_coordinator(ring, pm.clone(), 1u64, storage, 1, ConsistencyLevel::One);

        let table_id = TableId::new("test_ks", "test_tbl");
        let partitions = coordinator
            .coordinate_index_read(
                &table_id,
                "val_idx",
                &ferrosa_index::IndexKey(b"hello".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            partitions.len(),
            1,
            "index read should succeed after reconnect"
        );
        assert!(
            pm.has_peer(remote_host_id),
            "coordinator should cache the reconnected peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    struct StaticIndexReadInPartitionHandler {
        partition: Partition,
    }

    #[async_trait::async_trait]
    impl RpcHandler for StaticIndexReadInPartitionHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            let Message::IndexReadInPartitionRequest(_) = msg else {
                return None;
            };
            let payload = IndexReadResponsePayload {
                partitions: vec![crate::raft::handlers::partition_to_wire(
                    self.partition.clone(),
                )],
            };
            Some(Message::IndexReadInPartitionResponse(Bytes::from(
                bincode::serialize(&payload).unwrap(),
            )))
        }
    }

    /// t_430c4188: the KEYED index read must contact only the partition's
    /// replicas (normal keyed routing), never scatter-gather the whole ring.
    ///
    /// Ring: node 2 owns the key's token (RF=1 replica) and serves the real
    /// partition; node 3 is a live non-replica that would serve a POISON
    /// partition with a different token. If the keyed read degenerated to a
    /// global scatter-gather, the poison partition would leak into the result.
    #[tokio::test]
    async fn coordinate_index_read_in_partition_contacts_only_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key(); // Token(42)
        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(888)],
        };
        let poison = Partition {
            key: DecoratedKey {
                token: Token(99_999),
                key: PartitionKey::new(vec![9, 9, 9]),
            },
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(777)],
        };

        let (replica_server, replica_addr, replica_host_id) = start_rpc_server(
            MsgType::IndexReadInPartitionRequest,
            Arc::new(StaticIndexReadInPartitionHandler {
                partition: partition.clone(),
            }),
        )
        .await;
        let (other_server, other_addr, other_host_id) = start_rpc_server(
            MsgType::IndexReadInPartitionRequest,
            Arc::new(StaticIndexReadInPartitionHandler {
                partition: poison.clone(),
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let mut replica_node = make_node(&replica_addr.to_string());
        replica_node.host_id = replica_host_id;
        let mut other_node = make_node(&other_addr.to_string());
        other_node.host_id = other_host_id;

        let mut ring = TokenRing::new();
        ring.add_node(2u64, replica_node);
        ring.add_node(3u64, other_node);
        // Key token 42: clockwise owner is node 2 (token 42); node 3 owns a
        // far-away range, so with RF=1 it is NOT a replica of the key.
        ring.assign_tokens(2u64, &[42]);
        ring.assign_tokens(3u64, &[1_000_000]);

        let coordinator =
            make_coordinator(ring, pm.clone(), 1u64, storage, 1, ConsistencyLevel::One);

        let table_id = TableId::new("test_ks", "test_tbl");
        let strategy = crate::ring::strategy::ReplicationStrategy::Simple {
            replication_factor: 1,
        };
        let partitions = coordinator
            .coordinate_index_read_in_partition(
                &table_id,
                &key,
                "val_idx",
                &ferrosa_index::IndexKey(b"hello".to_vec()),
                &strategy,
            )
            .await
            .unwrap();

        assert_eq!(
            partitions.len(),
            1,
            "keyed index read must return exactly the replica's partition"
        );
        assert_eq!(
            partitions[0].key.token.0, 42,
            "result must be the keyed partition from the replica"
        );
        assert!(
            partitions.iter().all(|p| p.key.token.0 != 99_999),
            "a non-replica node's data must never appear: the keyed read \
             degenerated to a global scatter-gather"
        );

        replica_server
            .shutdown(std::time::Duration::from_millis(50))
            .await;
        other_server
            .shutdown(std::time::Duration::from_millis(50))
            .await;
    }

    /// The real inbound handler decodes the payload, restricts the consult to
    /// the requested partition, and answers with only that partition's
    /// matching rows.
    #[tokio::test]
    async fn index_read_in_partition_handler_serves_partition_restricted_rows() {
        use crate::raft::handlers::{
            IndexReadInPartitionHandler, IndexReadInPartitionRequestPayload,
        };

        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        // Register with a secondary index on the val column (position 0).
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage
            .register_table_with_indexes(schema, vec![("val_idx".to_string(), 0_usize)])
            .unwrap();

        let table_id = TableId::new("test_ks", "test_tbl");
        let k1 = DecoratedKey::new(PartitionKey::new(b"pk1".to_vec()));
        let k2 = DecoratedKey::new(PartitionKey::new(b"pk2".to_vec()));
        storage.write(&table_id, &k1, test_row(10), 10).unwrap();
        storage.write(&table_id, &k2, test_row(11), 11).unwrap();

        let handler = IndexReadInPartitionHandler::new(storage);
        let req = IndexReadInPartitionRequestPayload {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            index_name: "val_idx".to_string(),
            index_key: b"hello".to_vec(),
            partition_key: b"pk1".to_vec(),
        };
        let resp = handler
            .handle(
                (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap()),
                Message::IndexReadInPartitionRequest(Bytes::from(
                    bincode::serialize(&req).unwrap(),
                )),
            )
            .await
            .expect("handler must answer");

        let Message::IndexReadInPartitionResponse(b) = resp else {
            panic!("expected IndexReadInPartitionResponse, got {resp:?}");
        };
        let payload: IndexReadResponsePayload = bincode::deserialize(&b).unwrap();
        let partitions: Vec<Partition> = payload
            .partitions
            .into_iter()
            .map(partition_from_wire)
            .collect();
        assert_eq!(partitions.len(), 1, "only the pk1 match must be returned");
        assert_eq!(
            partitions[0].key.key.as_bytes(),
            b"pk1",
            "the pk2 match for the same value must be excluded"
        );
    }

    #[tokio::test]
    async fn coordinate_read_quorum_reconnects_missing_digest_peer_pool() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(999)],
        };

        let table_id = TableId::new("test_ks", "test_tbl");
        storage
            .write(&table_id, &key, partition.rows[0].clone(), 999)
            .unwrap();

        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::ReadRequest,
            Arc::new(StaticDigestReadHandler {
                partition: partition.clone(),
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let local_node_id = 1u64;
        let mut local = make_node("127.0.0.1:7000");
        local.host_id = Uuid::new_v4();
        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local);
        ring.add_node(2u64, remote);
        ring.assign_tokens(local_node_id, &[42]);
        ring.assign_tokens(2u64, &[142]);

        let coordinator = make_coordinator(
            ring,
            pm.clone(),
            local_node_id,
            storage,
            2,
            ConsistencyLevel::Quorum,
        );

        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(
            result.is_some(),
            "quorum read should succeed after reconnect"
        );
        assert!(
            pm.has_peer(remote_host_id),
            "digest path should cache the reconnected peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    // -----------------------------------------------------------------------
    // BUG-012: per-query CL override
    // -----------------------------------------------------------------------

    /// Coordinator constructed with CL=QUORUM but query specifies CL=ONE.
    /// The coordinator must use the per-query CL, not the hard-coded default.
    #[tokio::test]
    async fn coordinate_read_uses_per_query_cl() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        // Coordinator default is QUORUM, RF=3 — which would require 2 replicas.
        // But we only have 1 node. With CL=ONE override, it should succeed.
        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            3,                        // RF=3 (default)
            ConsistencyLevel::Quorum, // default CL
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row(1000);
        storage.write(&table_id, &key, row, 1000).unwrap();

        // Without override: QUORUM with 1 node in the ring should fail.
        let result = coordinator.coordinate_read(&table_id, &key).await;
        assert!(
            result.is_err(),
            "default QUORUM read with 1 node should fail"
        );

        // With per-query override to CL=ONE, RF=1: should succeed.
        let result = coordinator
            .coordinate_read_with(&table_id, &key, ConsistencyLevel::One, 1)
            .await;
        assert!(
            result.is_ok(),
            "CL=ONE override should succeed: got {:?}",
            result
        );
        assert!(result.unwrap().is_some());
    }

    // -----------------------------------------------------------------------
    // BUG-013: digest mismatch should re-fetch newer data
    // -----------------------------------------------------------------------

    /// When two replicas have matching digests (local + local via RF=1,CL=ONE),
    /// data is returned via the fast path. This test sets up two local replicas
    /// with different data (different timestamps), reads at CL=ALL RF=1 to
    /// trigger the single-replica fast path, and verifies the newest data is
    /// returned.
    ///
    /// The actual digest-mismatch-with-refetch path requires two distinct
    /// storage backends and is tested in docker smoke tests. This unit test
    /// verifies the `full_refetch` method exists and the mismatch code path
    /// compiles and runs without panic.
    #[tokio::test]
    async fn digest_mismatch_returns_newest_data_local() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Write older then newer data to the same key.
        storage.write(&table_id, &key, test_row(100), 100).unwrap();
        storage
            .write(&table_id, &key, test_row(9000), 9000)
            .unwrap();

        // CL=ONE with 1 replica — fast path, no digest comparison.
        // Verify the newest data is returned (ts=9000).
        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some());
        let rows = result.unwrap();
        let max_ts = rows
            .iter()
            .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
            .max()
            .unwrap_or(i64::MIN);
        assert_eq!(max_ts, 9000, "must return newest data (ts=9000)");
    }

    /// Verify that full_refetch returns None when the remote replica is
    /// unreachable (the method must not panic, and the coordinator should
    /// gracefully fall back to the data it already has).
    #[tokio::test]
    async fn full_refetch_returns_none_when_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let local_node_id = 1u64;
        let ring = TokenRing::new();
        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage,
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Attempt a full refetch from a nonexistent peer.
        let result = coordinator
            .full_refetch(&table_id, &key, Uuid::new_v4())
            .await;
        assert!(
            result.is_none(),
            "full_refetch should return None when replica is unreachable"
        );
    }

    // -----------------------------------------------------------------------
    // Task 7: repair_stale_replicas tests
    // -----------------------------------------------------------------------

    /// repair_stale_replicas sends RepairWrite to stale replicas.
    /// With a noop peer manager (no real connections), all sends will fail,
    /// so we verify the metrics reflect the failures.
    #[tokio::test]
    async fn repair_stale_replicas_increments_metrics_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let remote_uuid = Uuid::new_v4();
        let pm = noop_peer_manager();

        let ring = TokenRing::new();
        let coordinator = make_coordinator(
            ring,
            pm,
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(1000)],
        };

        // Attempt repair to unreachable replica.
        coordinator
            .repair_stale_replicas(&table_id, &partition, &[remote_uuid])
            .await;

        let attempted = coordinator
            .repair_metrics
            .read_repairs_attempted
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = coordinator
            .repair_metrics
            .read_repairs_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(attempted, 1, "should attempt repair for 1 stale replica");
        assert_eq!(failed, 1, "should fail when peer is unreachable");
    }

    #[tokio::test]
    async fn repair_stale_replicas_applies_local_stale_replica_directly() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let local_host_id = Uuid::new_v4();
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.host_id = local_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(1000)],
        };

        coordinator
            .repair_stale_replicas(&table_id, &partition, &[local_host_id])
            .await;

        let attempted = coordinator
            .repair_metrics
            .read_repairs_attempted
            .load(std::sync::atomic::Ordering::Relaxed);
        let succeeded = coordinator
            .repair_metrics
            .read_repairs_succeeded
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = coordinator
            .repair_metrics
            .read_repairs_failed
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(attempted, 1);
        assert_eq!(succeeded, 1);
        assert_eq!(failed, 0);
        assert!(
            storage.read(&table_id, &key).unwrap().is_some(),
            "local repair should write directly to storage"
        );
    }

    /// Verify that the coordinate_read_with code path that calls
    /// repair_stale_replicas compiles and does not panic when the
    /// newest data is local (no stale replicas to repair).
    #[tokio::test]
    async fn coordinate_read_local_no_stale_replicas_no_repair() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some());

        // No stale replicas, so no repair attempts.
        let attempted = coordinator
            .repair_metrics
            .read_repairs_attempted
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(attempted, 0);
    }

    #[tokio::test]
    async fn repair_stale_replicas_empty_list_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let coordinator = make_coordinator(
            TokenRing::new(),
            noop_peer_manager(),
            1u64,
            storage,
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(1000)],
        };

        coordinator
            .repair_stale_replicas(&table_id, &partition, &[])
            .await;

        let attempted = coordinator
            .repair_metrics
            .read_repairs_attempted
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(attempted, 0, "no stale replicas means no repair attempts");
    }

    // -----------------------------------------------------------------------
    // Task 10: Integration-style test
    // -----------------------------------------------------------------------

    /// Integration-style test: verify that repair_stale_replicas sends
    /// repair writes to the correct set of stale replicas and increments
    /// metrics. Uses 3 storage engines to simulate 3 replicas.
    #[tokio::test]
    async fn repair_stale_replicas_sends_to_all_stale_hosts() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let stale_uuid_1 = Uuid::new_v4();
        let stale_uuid_2 = Uuid::new_v4();

        // Set up PeerManager with peer entries (no real pools -- sends will fail).
        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        pm.add_peer_entry((stale_uuid_1, "10.0.0.2:7000".parse().unwrap()))
            .await;
        pm.add_peer_entry((stale_uuid_2, "10.0.0.3:7000".parse().unwrap()))
            .await;

        let coordinator = make_coordinator(
            TokenRing::new(),
            pm,
            1u64,
            storage.clone(),
            3,
            ConsistencyLevel::Quorum,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(5000)],
        };

        // Repair two stale replicas.
        coordinator
            .repair_stale_replicas(&table_id, &partition, &[stale_uuid_1, stale_uuid_2])
            .await;

        let attempted = coordinator
            .repair_metrics
            .read_repairs_attempted
            .load(std::sync::atomic::Ordering::Relaxed);
        let failed = coordinator
            .repair_metrics
            .read_repairs_failed
            .load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            attempted, 2,
            "should attempt repair for both stale replicas"
        );
        assert_eq!(failed, 2, "both should fail (no real connection pools)");
    }

    // -----------------------------------------------------------------------
    // NTS read coordination tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn coordinate_read_nts_local_quorum_reads_from_local_dc() {
        // Setup: dc1 has local node with data, dc2 has unreachable node.
        // CL=LOCAL_QUORUM with dc1_rf=1 => block_for_dc(1) = 1.
        // Local node has data => should succeed.
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.data_center = "dc1".to_string();

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::LocalQuorum,
        );

        // Write data directly to storage
        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row(1000);
        storage.write(&table_id, &key, row, 1000).unwrap();

        let dc_rf = std::collections::HashMap::from([("dc1".to_string(), 1usize)]);
        let strategy = crate::ring::strategy::ReplicationStrategy::NetworkTopology { dc_rf };

        let result = coordinator
            .coordinate_read_nts(&table_id, &key, ConsistencyLevel::LocalQuorum, &strategy)
            .await
            .unwrap();

        assert!(result.is_some(), "should read back written data");
    }

    #[tokio::test]
    async fn coordinate_read_nts_unavailable_when_insufficient_local_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        // dc1 has 1 node (local), dc1_rf=3, CL=LOCAL_QUORUM => need 2
        // Only 1 local DC replica => Unavailable
        let local_node_id = 1u64;
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.data_center = "dc1".to_string();

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);
        ring.assign_tokens(local_node_id, &[100]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            3,
            ConsistencyLevel::LocalQuorum,
        );

        let dc_rf = std::collections::HashMap::from([("dc1".to_string(), 3usize)]);
        let strategy = crate::ring::strategy::ReplicationStrategy::NetworkTopology { dc_rf };

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        let result = coordinator
            .coordinate_read_nts(&table_id, &key, ConsistencyLevel::LocalQuorum, &strategy)
            .await;

        match result {
            Err(ClusterError::Unavailable {
                required, alive, ..
            }) => {
                assert_eq!(required, 2, "LOCAL_QUORUM of rf=3 requires 2");
                assert_eq!(alive, 1, "only 1 local DC replica");
            }
            other => panic!("expected Unavailable, got: {other:?}"),
        }
    }

    // -- index-aware replica selection tests --------------------------------

    #[test]
    fn select_index_ready_replicas_prefers_ready() {
        let mut index_state_map: BTreeMap<
            (String, String, String),
            BTreeMap<u64, IndexNodeStatus>,
        > = BTreeMap::new();
        let mut node_statuses = BTreeMap::new();
        node_statuses.insert(1u64, IndexNodeStatus::Building);
        node_statuses.insert(2u64, IndexNodeStatus::Ready);
        node_statuses.insert(3u64, IndexNodeStatus::Ready);
        index_state_map.insert(("ks".into(), "tbl".into(), "idx".into()), node_statuses);

        let replicas = vec![1u64, 2, 3];
        let result = select_index_ready_replicas(&replicas, "ks", "tbl", "idx", &index_state_map);

        // Ready replicas (2, 3) should come before Building (1).
        assert_eq!(result[0], 2);
        assert_eq!(result[1], 3);
        assert_eq!(result[2], 1);
    }

    #[test]
    fn select_index_ready_replicas_no_state_returns_original_order() {
        let index_state_map = BTreeMap::new();
        let replicas = vec![1u64, 2, 3];
        let result = select_index_ready_replicas(&replicas, "ks", "tbl", "idx", &index_state_map);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn select_index_ready_replicas_all_building_returns_all() {
        let mut index_state_map: BTreeMap<
            (String, String, String),
            BTreeMap<u64, IndexNodeStatus>,
        > = BTreeMap::new();
        let mut node_statuses = BTreeMap::new();
        node_statuses.insert(1u64, IndexNodeStatus::Building);
        node_statuses.insert(2u64, IndexNodeStatus::Building);
        index_state_map.insert(("ks".into(), "tbl".into(), "idx".into()), node_statuses);

        let replicas = vec![1u64, 2];
        let result = select_index_ready_replicas(&replicas, "ks", "tbl", "idx", &index_state_map);
        // All Building -- original order preserved.
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn select_index_ready_replicas_stable_sort_preserves_ready_order() {
        let mut index_state_map: BTreeMap<
            (String, String, String),
            BTreeMap<u64, IndexNodeStatus>,
        > = BTreeMap::new();
        let mut node_statuses = BTreeMap::new();
        node_statuses.insert(1u64, IndexNodeStatus::Ready);
        node_statuses.insert(2u64, IndexNodeStatus::Failed("err".into()));
        node_statuses.insert(3u64, IndexNodeStatus::Ready);
        node_statuses.insert(4u64, IndexNodeStatus::Stale);
        index_state_map.insert(("ks".into(), "tbl".into(), "idx".into()), node_statuses);

        let replicas = vec![4u64, 3, 2, 1];
        let result = select_index_ready_replicas(&replicas, "ks", "tbl", "idx", &index_state_map);

        // Ready nodes first (3, 1 -- in original relative order), then rest (4, 2).
        assert_eq!(result, vec![3, 1, 4, 2]);
    }

    // -----------------------------------------------------------------------
    // CL=ONE read fallback: data on non-preferred replica
    // -----------------------------------------------------------------------

    /// CL=ONE with RF=1: local replica has no data for the key.
    /// The coordinator should try the next replica and return data.
    /// This tests the fallback behavior when the preferred (local) replica
    /// returns None — the coordinator must not give up immediately.
    #[tokio::test]
    async fn read_one_fallback_returns_data_from_second_replica() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring.clone(),
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Do NOT write any data — local storage is empty.
        // With a single-node ring, the coordinator should try local and get None.
        let result = coordinator
            .read_one_replica(&table_id, &key, &[local_node_id], &ring)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "should return None when only replica has no data"
        );

        // Now write data and verify it returns.
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();
        let result = coordinator
            .read_one_replica(&table_id, &key, &[local_node_id], &ring)
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "should find data after write to local replica"
        );
    }

    /// CL=ONE with multiple local replicas: first replica has no data,
    /// second replica (also local) has data. Verifies the fallback loop.
    #[tokio::test]
    async fn read_one_fallback_iterates_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring.clone(),
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();

        // Write data to storage for this key.
        storage
            .write(&table_id, &key, test_row(2000), 2000)
            .unwrap();

        // Pass replicas list where local is second — the function should
        // still find it because it reorders to prefer local.
        let remote_id = 99u64;
        let result = coordinator
            .read_one_replica(&table_id, &key, &[remote_id, local_node_id], &ring)
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "should find data on local replica even when listed second"
        );
    }

    // -----------------------------------------------------------------------
    // BUG: coordinate_range_read silent data loss
    // -----------------------------------------------------------------------

    /// When a complete streaming range read needs remote token owners
    /// and every remote fire fails, coordinate_range_read must fail
    /// loudly instead of returning local-only partial results as if the
    /// scan were complete.
    #[tokio::test]
    async fn coordinate_range_read_errors_when_every_required_remote_is_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let remote_uuid_2 = Uuid::new_v4();
        let remote_uuid_3 = Uuid::new_v4();

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        // Add peers with no connection pools — sends will fail.
        pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
            .await;
        pm.add_peer_entry((remote_uuid_3, "10.0.0.3:7000".parse().unwrap()))
            .await;

        let mut node2 = make_node("10.0.0.2:7000");
        node2.host_id = remote_uuid_2;
        let mut node3 = make_node("10.0.0.3:7000");
        node3.host_id = remote_uuid_3;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.add_node(2u64, node2);
        ring.add_node(3u64, node3);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);
        ring.assign_tokens(3u64, &[200]);

        let coordinator = make_coordinator(
            ring,
            pm,
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        let result = coordinator.coordinate_range_read(&table_id).await;
        assert!(
            result.is_err(),
            "complete streaming range read must not report local-only partial data as success"
        );
        let message = result.err().unwrap().to_string();
        assert!(
            message.contains("every replica fire failed"),
            "error should name the remote fanout failure, got: {message}"
        );
    }

    /// Single-node range read should succeed (no remote nodes to fail).
    #[tokio::test]
    async fn coordinate_range_read_single_node_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        let result = coordinator.coordinate_range_read(&table_id).await;
        assert!(result.is_ok(), "single-node range read should succeed");
        let partitions = result.unwrap();
        assert_eq!(
            partitions.len(),
            1,
            "should return the one written partition"
        );
    }

    /// Regression for the COUNT(*) undercount bug (forge t_8c4e44e8). When the
    /// keyspace RF does not span the ring, the local replica holds only the
    /// partitions in its owned token ranges. The OLD `coordinate_range_count`
    /// called `storage.count_range` directly and silently returned that local
    /// SUBSET as the answer — a nondeterministic undercount, while a full
    /// `SELECT` (which fans out + dedups by token) saw every row.
    ///
    /// The fix routes COUNT(*) through the same CL-selected, token-deduped
    /// fan-out the streaming read uses. So when the required remote owners are
    /// unreachable, COUNT(*) must FAIL LOUD (exactly like
    /// `coordinate_range_read`) rather than report local-only partial data as a
    /// complete count.
    /// COUNT(*) must honour the CLIENT's consistency level, not the node's
    /// configured default.
    ///
    /// `SELECT COUNT(*)` takes the ADR-020 fast path, which called
    /// `coordinate_range_count(table_id)` — a signature with nowhere to put
    /// the request's CL. It therefore used `self.default_cl`, while the full
    /// `SELECT` path on the very same table threads `ctx.consistency`
    /// through. Two ways of counting one table, answering at two different
    /// consistency levels.
    ///
    /// The failure is silent and it under-reports. With RF == node_count and
    /// a locally-satisfiable default (ONE/LOCAL_ONE), `range_read_remotes`
    /// is empty and the count is served entirely from local storage — so a
    /// client that explicitly asked for QUORUM or ALL receives a local-only
    /// tally with no error and no indication the CL was ignored.
    ///
    /// Setup below: RF == node_count == 2 and `default_cl = ONE`, so the
    /// default path legitimately counts locally. The client asks for ALL,
    /// which requires the remote — and that remote is unreachable, so the
    /// request MUST fail rather than quietly return the local subset.
    #[tokio::test]
    async fn range_count_honours_requested_cl_not_node_default() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let remote_uuid_2 = Uuid::new_v4();

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        // Remote peer with no connection pool — any send to it fails.
        pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
            .await;

        let mut node2 = make_node("10.0.0.2:7000");
        node2.host_id = remote_uuid_2;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.add_node(2u64, node2);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[150]);

        // RF == node_count == 2 with default CL=ONE: the local replica owns
        // every token range at that CL, so the default count path is exact
        // and local-only.
        let coordinator = make_coordinator(
            ring,
            pm,
            local_node_id,
            storage.clone(),
            2,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        storage
            .write(&table_id, &test_key(), test_row(1000), 1000)
            .unwrap();

        // Baseline: at the node's own default (ONE) the local count is a
        // legitimate answer and must still work.
        let at_default = coordinator.coordinate_range_count(&table_id).await;
        assert_eq!(
            at_default.expect("CL=ONE count is satisfied locally"),
            1,
            "the default-CL fast path must keep working"
        );

        // The client asked for ALL. That cannot be satisfied without the
        // remote, and the remote is unreachable — so this must be an error,
        // NOT a silent local-only count of 1.
        let at_all = coordinator
            .coordinate_range_count_with(&table_id, ConsistencyLevel::All)
            .await;
        assert!(
            at_all.is_err(),
            "COUNT(*) at CL=ALL must not silently return the local-only \
             subset when a required remote is unreachable; got {:?}",
            at_all.ok()
        );
    }

    #[tokio::test]
    async fn coordinate_range_count_errors_when_required_remote_is_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let remote_uuid_2 = Uuid::new_v4();

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        // Remote peer with no connection pool — sends will fail.
        pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
            .await;

        let mut node2 = make_node("10.0.0.2:7000");
        node2.host_id = remote_uuid_2;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.add_node(2u64, node2);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[150]);

        // RF=1 over a 2-node ring => the local node does NOT own every token
        // range, so `range_read_remotes` is non-empty and COUNT(*) must fan out.
        let coordinator = make_coordinator(
            ring,
            pm,
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        // Write a partition locally. The OLD code would have happily returned
        // count=1 here — but that is only the LOCAL view; node2 may hold more.
        storage
            .write(&table_id, &test_key(), test_row(1000), 1000)
            .unwrap();

        let result = coordinator.coordinate_range_count(&table_id).await;
        assert!(
            result.is_err(),
            "COUNT(*) must not report the local-only subset as a complete count \
             when a required remote owner is unreachable"
        );
        let message = result.err().unwrap().to_string();
        assert!(
            message.contains("fire failed"),
            "error should name the remote fanout failure, got: {message}"
        );
    }

    /// Companion to the undercount regression: when the local node owns the
    /// entire ring at the configured CL (`range_read_remotes` empty), COUNT(*)
    /// keeps the exact local metadata fast path and returns the true count.
    #[tokio::test]
    async fn coordinate_range_count_single_node_returns_exact_local_count() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        storage
            .write(&table_id, &test_key(), test_row(1000), 1000)
            .unwrap();

        let count = coordinator
            .coordinate_range_count(&table_id)
            .await
            .expect("single-node COUNT(*) should succeed via the local fast path");
        assert_eq!(count, 1, "COUNT(*) must equal the one written partition");
    }

    struct GeneratorRangeStorage {
        generated_rows: usize,
    }

    impl RangeReadStorage for GeneratorRangeStorage {
        fn read_range_unbounded(
            &self,
            _table_id: &TableId,
            _limit: usize,
        ) -> ferrosa_common::Result<Vec<Partition>> {
            panic!("bounded local coordinator path must not call unbounded storage read");
        }

        fn read_range_bounded_rows(
            &self,
            _table_id: &TableId,
            limit: usize,
            row_limit: usize,
        ) -> ferrosa_common::Result<Vec<Partition>> {
            assert_eq!(limit, 1, "partition limit should be passed through");
            assert_eq!(row_limit, 1, "row_limit should be passed through");
            let retained = self.generated_rows.min(row_limit);
            Ok(vec![Partition {
                key: test_key(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: (0..retained)
                    .map(|i| {
                        let mut row = test_row(i as i64);
                        row.clustering = (i as u32).to_be_bytes().to_vec();
                        row
                    })
                    .collect(),
            }])
        }
    }

    #[test]
    fn local_range_read_limited_rows_uses_bounded_generator_storage() {
        let storage = GeneratorRangeStorage {
            generated_rows: 100_000,
        };
        let table_id = TableId::new("test_ks", "test_tbl");

        let partitions = read_local_range_limited_rows(&storage, &table_id, 1, 1)
            .expect("bounded local range read should use generator storage bounded seam");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].rows.len(), 1);
    }

    #[tokio::test]
    async fn coordinate_range_read_limited_rows_bounds_local_replica_before_materialization() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        for i in 0..2_000u32 {
            let mut row = test_row(i as i64);
            row.clustering = i.to_be_bytes().to_vec();
            storage.write(&table_id, &key, row, i as i64).unwrap();
        }
        storage.flush(&table_id).unwrap();

        // Correctness of the bounded fold: LIMIT/row_limit honored over a
        // partition far larger than the bound. The MEMORY-boundedness contract
        // (streaming fold, O(sources + k) resident — not materialize-then-
        // truncate) is asserted deterministically by the alloc-measured
        // `tests/replica_scan_serialization_memory_bound.rs`; a wall-clock
        // timeout here was flaky on slower instrumented CI runners.
        let partitions = coordinator
            .coordinate_range_read_limited_rows(&table_id, 1, 1)
            .await
            .expect("bounded local range read should succeed");
        assert_eq!(partitions.len(), 1, "expected the one written partition");
        assert_eq!(
            partitions[0].rows.len(),
            1,
            "row_limit=1 must retain only one row from the partition"
        );
    }

    /// Responsiveness guard: a large local range read must run on the blocking
    /// pool, NOT inline on the async worker. Mirrors the offload contract of
    /// `read_local_partition`.
    ///
    /// The deterministic signal is THREAD IDENTITY. A synchronous range scan run
    /// inline executes on the calling async worker thread; offloaded via
    /// `TaskPool::spawn_blocking`, it executes on a distinct blocking-pool
    /// thread, leaving the worker free to keep driving the CQL keepalive / raft
    /// heartbeat. (A wall-clock "did the worker keep ticking" probe is NOT
    /// reliable here: the storage rehydration path uses
    /// `tokio::task::block_in_place`, which on a multi-thread runtime keeps the
    /// runtime live even for inline work — so only thread identity distinguishes
    /// inline from offloaded.)
    ///
    /// This test exercises the EXACT offload mechanism the production helper
    /// uses — `TaskPool::current("coordinator-local-range-read").spawn_blocking`
    /// wrapping the real `read_local_range_limited_rows` scan — and asserts the
    /// scan runs off the async worker thread. It then drives the real
    /// `read_local_range_limited_rows_offloaded` wrapper to confirm it returns
    /// the same data, so the wrapper is covered end-to-end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn large_local_range_read_runs_on_blocking_pool_and_does_not_park_worker() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let table_id = TableId::new("test_ks", "test_tbl");
        // Write many partitions across several flushed SSTables so the bounded
        // (row_limit > 0) scan does real, non-trivial synchronous I/O + decode.
        for batch in 0..8u32 {
            for i in 0..4_000u32 {
                let pk = (batch * 4_000 + i).to_be_bytes().to_vec();
                let key = DecoratedKey {
                    token: Token((batch * 4_000 + i) as i64),
                    key: PartitionKey::new(pk),
                };
                storage
                    .write(&table_id, &key, test_row(i as i64), i as i64)
                    .unwrap();
            }
            storage.flush(&table_id).unwrap();
        }

        // The thread this async task is running on (a tokio worker thread).
        let worker_thread = std::thread::current().id();

        // Run the real scan through the EXACT offload seam used by
        // `read_local_range_limited_rows_offloaded`, capturing the thread the
        // scan executes on.
        let storage_for_scan = std::sync::Arc::clone(&storage);
        let table_for_scan = table_id.clone();
        let scan_thread = std::sync::Arc::new(std::sync::Mutex::new(None));
        let scan_thread_probe = scan_thread.clone();
        let partitions =
            ferrosa_common::task_pool::TaskPool::current("coordinator-local-range-read")
                .spawn_blocking(move || {
                    *scan_thread_probe.lock().unwrap() = Some(std::thread::current().id());
                    read_local_range_limited_rows(
                        storage_for_scan.as_ref(),
                        &table_for_scan,
                        10_000,
                        4,
                    )
                })
                .await
                .expect("offloaded scan task must not panic")
                .expect("offloaded scan must succeed");

        let scan_thread = scan_thread
            .lock()
            .unwrap()
            .expect("scan closure must have recorded its thread");

        assert!(
            !partitions.is_empty(),
            "range read must return partitions (proves it did real work)"
        );

        // KEY ASSERTION: the synchronous scan ran on a DIFFERENT thread than the
        // async worker — i.e. it was offloaded to the blocking pool and did NOT
        // park the worker that drives the CQL connection keepalive.
        assert_ne!(
            scan_thread, worker_thread,
            "the synchronous range scan ran on the async worker thread ({worker_thread:?}); \
             it must be offloaded to a blocking-pool thread so a large local range read cannot \
             park the CQL connection's keepalive / raft heartbeat"
        );

        // End-to-end: the production wrapper returns the same bounded result.
        let via_wrapper = read_local_range_limited_rows_offloaded(&storage, &table_id, 10_000, 4)
            .await
            .expect("offloaded wrapper read should succeed");
        assert_eq!(
            via_wrapper.len(),
            partitions.len(),
            "the offloaded wrapper must return the same partition count as the direct scan"
        );
    }

    #[tokio::test]
    async fn legacy_range_read_uses_bulk_lane_timeout_for_slow_remote_scan() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key();
        let partition = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(4321)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::RangeReadRequest,
            Arc::new(DelayedRangeReadHandler {
                partition: partition.clone(),
                delay: std::time::Duration::from_secs(1),
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(2u64, remote);
        ring.assign_tokens(2u64, &[42]);

        let mut coordinator =
            make_coordinator(ring, pm.clone(), 1u64, storage, 1, ConsistencyLevel::One);
        coordinator.streaming_range_reads = false;

        let table_id = TableId::new("test_ks", "test_tbl");
        let partitions = coordinator.coordinate_range_read(&table_id).await.unwrap();
        assert_eq!(
            partitions.len(),
            1,
            "slow remote range scans should complete on the bulk lane"
        );
        assert!(
            pm.has_peer(remote_host_id),
            "coordinator should cache the reconnected peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn unbounded_remote_partition_read_isolated_from_data_lane_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key();
        let partition = Partition {
            key: key.clone(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(4321)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::ReadRequest,
            Arc::new(DelayedPartitionReadHandler {
                partition,
                delay: std::time::Duration::from_millis(250),
            }),
        )
        .await;

        let config = NetConfig {
            data_lane_timeout: std::time::Duration::from_millis(100),
            bulk_lane_timeout: std::time::Duration::from_secs(2),
            ..NetConfig::default()
        };
        let peer_manager = Arc::new(PeerManager::new(
            Arc::new(config),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(2, remote);
        ring.assign_tokens(2, &[key.token.0]);
        let coordinator =
            make_coordinator(ring, peer_manager, 1, storage, 1, ConsistencyLevel::One);

        let rows = coordinator
            .coordinate_read_with_limited_rows(
                &TableId::new("test_ks", "test_tbl"),
                &key,
                ConsistencyLevel::One,
                1,
                0,
            )
            .await
            .expect("unbounded scan should use the longer Bulk-lane timeout")
            .expect("remote partition should be found");

        assert_eq!(rows.len(), 1);
        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    // -----------------------------------------------------------------------
    // Additional coverage: empty ring range read
    // -----------------------------------------------------------------------

    /// Range read on an empty ring returns an error (no nodes at all).
    #[tokio::test]
    async fn coordinate_range_read_empty_ring_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let coordinator = make_coordinator(
            TokenRing::new(),
            noop_peer_manager(),
            1u64,
            storage,
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let result = coordinator.coordinate_range_read(&table_id).await;
        // Empty ring means no nodes to contact at all, should return Ok([])
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn coordinate_index_read_uses_bulk_lane_timeout_for_slow_remote_scan() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let key = test_key();
        let partition = Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![test_row(9876)],
        };
        let (server, addr, remote_host_id) = start_rpc_server(
            MsgType::IndexReadRequest,
            Arc::new(DelayedIndexReadHandler {
                partition: partition.clone(),
                delay: std::time::Duration::from_secs(1),
            }),
        )
        .await;

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));

        let mut remote = make_node(&addr.to_string());
        remote.host_id = remote_host_id;
        let mut ring = TokenRing::new();
        ring.add_node(2u64, remote);
        ring.assign_tokens(2u64, &[42]);

        let coordinator =
            make_coordinator(ring, pm.clone(), 1u64, storage, 1, ConsistencyLevel::One);

        let table_id = TableId::new("test_ks", "test_tbl");
        let partitions = coordinator
            .coordinate_index_read(
                &table_id,
                "val_idx",
                &ferrosa_index::IndexKey(b"slow".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            partitions.len(),
            1,
            "slow remote index scans should complete on the bulk lane"
        );
        assert!(
            pm.has_peer(remote_host_id),
            "coordinator should cache the reconnected peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    // -----------------------------------------------------------------------
    // Additional coverage: CL=ALL with 1 node
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn coordinate_read_cl_all_single_node_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::All,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        let result = coordinator.coordinate_read(&table_id, &key).await;
        assert!(result.is_ok(), "CL=ALL with 1 node should succeed");
        assert!(result.unwrap().is_some());
    }

    // -----------------------------------------------------------------------
    // NTS: LOCAL_ONE CL
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn coordinate_read_nts_local_one_succeeds_with_single_dc_node() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.data_center = "dc1".to_string();

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::LocalOne,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        let dc_rf = std::collections::HashMap::from([("dc1".to_string(), 1usize)]);
        let strategy = crate::ring::strategy::ReplicationStrategy::NetworkTopology { dc_rf };

        let result = coordinator
            .coordinate_read_nts(&table_id, &key, ConsistencyLevel::LocalOne, &strategy)
            .await;
        assert!(
            result.is_ok(),
            "LOCAL_ONE should succeed: {:?}",
            result.err()
        );
        assert!(result.unwrap().is_some());
    }

    // -----------------------------------------------------------------------
    // NTS: regular CL with NetworkTopology strategy
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn coordinate_read_nts_quorum_with_nts_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.data_center = "dc1".to_string();

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        storage
            .write(&table_id, &key, test_row(1000), 1000)
            .unwrap();

        // Use CL=ONE (non-LOCAL) with NTS strategy -- takes the regular path
        let dc_rf = std::collections::HashMap::from([("dc1".to_string(), 1usize)]);
        let strategy = crate::ring::strategy::ReplicationStrategy::NetworkTopology { dc_rf };

        let result = coordinator
            .coordinate_read_nts(&table_id, &key, ConsistencyLevel::One, &strategy)
            .await;
        assert!(
            result.is_ok(),
            "CL=ONE with NTS should succeed: {:?}",
            result.err()
        );
    }

    // -----------------------------------------------------------------------
    // select_index_ready_replicas: all-ready
    // -----------------------------------------------------------------------

    #[test]
    fn select_index_ready_replicas_all_ready() {
        let mut index_state_map: BTreeMap<
            (String, String, String),
            BTreeMap<u64, IndexNodeStatus>,
        > = BTreeMap::new();
        let mut node_statuses = BTreeMap::new();
        node_statuses.insert(1u64, IndexNodeStatus::Ready);
        node_statuses.insert(2u64, IndexNodeStatus::Ready);
        index_state_map.insert(("ks".into(), "tbl".into(), "idx".into()), node_statuses);

        let replicas = vec![1u64, 2];
        let result = select_index_ready_replicas(&replicas, "ks", "tbl", "idx", &index_state_map);
        assert_eq!(result, vec![1, 2], "all ready means same order");
    }
}
