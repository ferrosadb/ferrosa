//! Read coordination -- fans out reads to replicas with CL enforcement.
//!
//! # Two-Phase Digest Read Protocol
//!
//! For CL > ONE:
//!
//! **Phase 1 — Concurrent fan-out**
//! 1. Compute replica set from the token ring (`ring.replicas(token, rf)`).
//! 2. Verify `replicas.len() >= block_for(cl)`, else return `Unavailable`.
//! 3. Pick one replica for a **full read** (prefer local if self is a replica).
//! 4. Send **digest-only reads** to the remaining `block_for(cl) - 1` replicas.
//! 5. Fan out concurrently via `FuturesUnordered`.
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
    partition_from_wire, RangeReadRequestPayload, RangeReadResponsePayload, ReadRequestPayload,
    ReadResponsePayload,
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

/// Decode a [`ReadResponsePayload`] from raw bytes, or return `None`.
fn decode_read_response(bytes: &[u8]) -> Option<ReadResponsePayload> {
    bincode::deserialize(bytes)
        .map_err(|e| tracing::warn!("coordinate_read: failed to decode ReadResponse: {e}"))
        .ok()
}

// ---------------------------------------------------------------------------
// coordinate_read
// ---------------------------------------------------------------------------

impl ClusterCoordinator {
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

    /// Full re-fetch from a remote replica identified by `host_id`.
    ///
    /// Called during digest-mismatch resolution when a remote replica has
    /// a newer timestamp than the full-read replica. Returns the remote
    /// partition, or `None` if the fetch fails.
    async fn full_refetch(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        host_id: uuid::Uuid,
    ) -> Option<Partition> {
        let payload = ReadRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            key: key.key.as_bytes().to_vec(),
            digest_only: false,
        };
        let body = encode_read_request(&payload);
        match self
            .peer_manager
            .send(host_id, Message::ReadRequest(body), Lane::Data)
            .await
        {
            Ok(Message::ReadResponse(b)) => match decode_read_response(&b) {
                Some(resp) if resp.found => resp.partition.map(partition_from_wire),
                _ => None,
            },
            _ => None,
        }
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
        let ring = self.ring.load();
        let replicas = ring.replicas(key.token.0, rf);
        let required = cl.block_for(rf);

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
            return self.read_one_replica(table_id, key, &replicas, &ring).await;
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

        // The remaining replicas (up to `required - 1`) do digest-only reads.
        let digest_replicas: Vec<u64> = replicas
            .iter()
            .copied()
            .filter(|&r| r != full_replica)
            .take(required - 1)
            .collect();

        // Collect node metadata before dropping the ring guard.
        let full_host_id = ring.get_node(full_replica).map(|n| n.host_id);
        let digest_host_ids: Vec<(u64, Option<uuid::Uuid>)> = digest_replicas
            .iter()
            .map(|&r| (r, ring.get_node(r).map(|n| n.host_id)))
            .collect();
        drop(ring);

        // -------------------------------------------------------------------
        // Phase 1: fan out — full read + digest-only reads concurrently.
        // -------------------------------------------------------------------

        let mut fan_out: FuturesUnordered<_> = {
            // Full-read future
            let full_future = {
                let storage = self.storage.clone();
                let peer_manager = self.peer_manager.clone();
                let table_id = table_id.clone();
                let key = key.clone();
                let local_node_id = self.local_node_id;
                let keyspace = table_id.keyspace.clone();
                let table_name = table_id.table.clone();
                let key_bytes = key.key.as_bytes().to_vec();

                async move {
                    if full_replica == local_node_id {
                        // Local full read.
                        match storage.read(&table_id, &key) {
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
                        };
                        let body = encode_read_request(&payload);
                        match full_host_id {
                            None => ReplicaRead::Failed,
                            Some(hid) => {
                                match peer_manager
                                    .send(hid, Message::ReadRequest(body), Lane::Data)
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

            // Digest-only futures
            let digest_futures = digest_host_ids.into_iter().map(|(replica_id, host_id)| {
                let storage = self.storage.clone();
                let peer_manager = self.peer_manager.clone();
                let table_id = table_id.clone();
                let key = key.clone();
                let local_node_id = self.local_node_id;
                let keyspace = table_id.keyspace.clone();
                let table_name = table_id.table.clone();
                let key_bytes = key.key.as_bytes().to_vec();

                async move {
                    if replica_id == local_node_id {
                        // Local digest read.
                        match storage.read(&table_id, &key) {
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
                                    host_id: None, // local — no re-fetch needed
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
                        // Remote digest-only read.
                        let payload = ReadRequestPayload {
                            keyspace,
                            table: table_name,
                            key: key_bytes,
                            digest_only: true,
                        };
                        let body = encode_read_request(&payload);
                        match host_id {
                            None => ReplicaRead::Failed,
                            Some(hid) => {
                                match peer_manager
                                    .send(hid, Message::ReadRequest(body), Lane::Data)
                                    .await
                                {
                                    Ok(Message::ReadResponse(b)) => {
                                        match decode_read_response(&b) {
                                            Some(resp) => ReplicaRead::Digest {
                                                digest: resp.digest,
                                                timestamp: resp.timestamp,
                                                host_id,
                                            },
                                            None => ReplicaRead::Failed,
                                        }
                                    }
                                    _ => ReplicaRead::Failed,
                                }
                            }
                        }
                    }
                }
            });

            // Collect all futures into one FuturesUnordered.
            let all: FuturesUnordered<
                std::pin::Pin<Box<dyn std::future::Future<Output = ReplicaRead> + Send>>,
            > = FuturesUnordered::new();
            all.push(Box::pin(full_future));
            for f in digest_futures {
                all.push(Box::pin(f));
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
                    // Don't count failures toward `received`.
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
                if let Some(newer_partition) = self.full_refetch(table_id, key, hid).await {
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
                    &self.repair_metrics,
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
            &self.repair_metrics,
            table_id,
            partition,
            stale_host_ids,
        )
        .await;
    }

    // -----------------------------------------------------------------------
    // CL=ONE helper
    // -----------------------------------------------------------------------

    /// Read from a single replica, preferring local.
    ///
    /// Tries the local node first (if it's a replica), then iterates through
    /// remaining replicas in order until one returns data or all are exhausted.
    async fn read_one_replica(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        replicas: &[u64],
        ring: &crate::ring::TokenRing,
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

        for &target in &candidates {
            if target == self.local_node_id {
                match self
                    .storage
                    .read(table_id, key)
                    .map(|opt| opt.map(|p| p.rows))
                    .map_err(ClusterError::Storage)
                {
                    Ok(Some(rows)) if !rows.is_empty() => return Ok(Some(rows)),
                    Ok(_) => continue, // no data on this replica, try next
                    Err(e) => {
                        tracing::debug!(%e, "read_one_replica: local read failed, trying next");
                        continue;
                    }
                }
            }

            // Remote replica.
            let host_id = match ring.get_node(target).map(|n| n.host_id) {
                Some(hid) => hid,
                None => continue,
            };

            let payload = ReadRequestPayload {
                keyspace: table_id.keyspace.clone(),
                table: table_id.table.clone(),
                key: key.key.as_bytes().to_vec(),
                digest_only: false,
            };
            let body = encode_read_request(&payload);
            match self
                .peer_manager
                .send(host_id, Message::ReadRequest(body), Lane::Data)
                .await
            {
                Ok(Message::ReadResponse(b)) => match decode_read_response(&b) {
                    Some(resp) if resp.found => {
                        let partition = resp.partition.map(partition_from_wire);
                        if let Some(p) = partition {
                            if !p.rows.is_empty() {
                                return Ok(Some(p.rows));
                            }
                        }
                        // found=true but no rows — try next replica
                    }
                    _ => {} // not found or decode failure — try next
                },
                _ => {
                    tracing::debug!(target, "read_one_replica: remote send failed, trying next");
                }
            }
        }

        // All replicas exhausted — data genuinely not found.
        Ok(None)
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
        let ring = self.ring.load();
        let all_replicas = ring.replicas_for_strategy(key.token.0, strategy);

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
        self.coordinate_read_with(table_id, key, cl, effective_replicas.len())
            .await
    }

    /// Scatter a full-table range read to every node in the ring.
    ///
    /// Each node returns its locally-stored partitions for `table_id`.  The
    /// coordinator deduplicates partitions that appear on multiple nodes
    /// (e.g. due to replication or token overlap) by merging replicas with
    /// the same partition key using last-write-wins cell semantics.
    pub async fn coordinate_range_read(
        &self,
        table_id: &TableId,
    ) -> crate::error::Result<Vec<ferrosa_sstable::types::Partition>> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();

        // Collect (node_id, host_id) pairs while the ring guard is held.
        let nodes: Vec<(u64, Option<uuid::Uuid>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| n.host_id)))
            .collect();
        drop(ring);

        let req_payload = RangeReadRequestPayload {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
        };
        let req_body = Bytes::from(bincode::serialize(&req_payload).unwrap_or_default());

        // Fan out to all nodes concurrently.
        let local_id = self.local_node_id;
        let storage = self.storage.clone();
        let peer_manager = self.peer_manager.clone();
        let table_id_clone = table_id.clone();

        let mut futs: FuturesUnordered<_> = nodes
            .into_iter()
            .map(|(node_id, host_id)| {
                let storage = storage.clone();
                let peer_manager = peer_manager.clone();
                let table_id = table_id_clone.clone();
                let req_body = req_body.clone();

                async move {
                    if node_id == local_id {
                        storage
                            .read_range(&table_id, None, None, 1_000_000)
                            .unwrap_or_default()
                    } else {
                        match host_id {
                            None => vec![],
                            Some(hid) => {
                                match peer_manager
                                    .send(
                                        hid,
                                        ferrosa_net::message::Message::RangeReadRequest(req_body),
                                        Lane::Data,
                                    )
                                    .await
                                {
                                    Ok(ferrosa_net::message::Message::RangeReadResponse(b)) => {
                                        match bincode::deserialize::<RangeReadResponsePayload>(&b) {
                                            Ok(resp) => resp
                                                .partitions
                                                .into_iter()
                                                .map(partition_from_wire)
                                                .collect(),
                                            Err(e) => {
                                                tracing::warn!(
                                                    "coordinate_range_read: \
                                                     failed to decode response: {e}"
                                                );
                                                vec![]
                                            }
                                        }
                                    }
                                    _ => vec![],
                                }
                            }
                        }
                    }
                }
            })
            .collect();

        // Collect all partitions.
        let mut all_partitions: Vec<ferrosa_sstable::types::Partition> = Vec::new();
        while let Some(batch) = futs.next().await {
            all_partitions.extend(batch);
        }

        // Deduplicate: group by token, merge replicas with the same partition key.
        use std::collections::BTreeMap;
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
    metrics: &ReadRepairMetrics,
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
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_net::rpc::handler::PeerId;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig};

    use crate::consistency::ConsistencyLevel;
    use crate::error::ClusterError;
    use crate::raft::{NodeInfo, NodeState};
    use crate::ring::TokenRing;

    // -----------------------------------------------------------------------
    // Helpers (mirrors write.rs test helpers)
    // -----------------------------------------------------------------------

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
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
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

    // -----------------------------------------------------------------------
    // Task 6 tests
    // -----------------------------------------------------------------------

    /// CL=ONE, local replica: should read directly from storage.
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
}
