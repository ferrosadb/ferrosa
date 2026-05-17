//! ADR-020 streaming variant of `coordinate_range_read_limited_rows`.
//!
//! Replaces the legacy single-shot `RangeReadRequest` per-replica
//! RPC with a multi-message streaming RPC keyed by `request_id`.
//! Local reads stay direct (no internode hop); each remote replica
//! receives a `RangeReadStreamRequest` and streams chunks back via
//! `Lane::Bulk` until a `RangeReadStreamDone` terminator. The
//! coordinator's `StreamRouter` dispatches the inbound chunks to
//! the per-request `mpsc::Receiver`; `consume_range_stream`
//! assembles them under an `IdleTimeoutWatchdog`.
//!
//! Wall-clock is unbounded — PB-scale scans take as long as they
//! take. Only genuine stalls (peer crashed mid-stream, network
//! partition with no further chunks or heartbeats) abort the
//! consume.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;

use super::read::read_local_range_limited_rows;
use super::stream_consumer::{consume_range_stream, StreamConsumeError};
use super::ClusterCoordinator;
use crate::error::ClusterError;
use crate::raft::handlers::RangeReadStreamRequestPayload;

/// Idle deadline on the streaming receiver. Reset on every chunk OR
/// heartbeat. A producer that stops sending entirely for longer
/// than this aborts the consume. Tunable later via NetConfig.
///
/// The Phase 1 handler emits a heartbeat every 3 s while a slow
/// storage read blocks (see stream_request_handler::HEARTBEAT_INTERVAL).
/// 30 s leaves room for runtime starvation under heavy concurrent
/// compaction load — if the handler can't even get scheduled for
/// 30 s the peer is genuinely stuck and aborting is correct.
const STREAMING_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-request buffer for the StreamRouter receiver. Bounded so a
/// slow consumer back-pressures the inbound dispatch (chunks queue
/// up at the lane until the consumer drains).
const STREAM_RECEIVER_BUFFER: usize = 32;

/// Default per-chunk partition count emitted by the streaming
/// range-read handler. Picked so each chunk message fits comfortably
/// inside the Bulk lane MTU envelope even for wide partitions, while
/// still amortizing the per-message frame overhead. Tunable later
/// via NetConfig.
pub const STREAMING_CHUNK_PARTITIONS: usize = 64;

impl ClusterCoordinator {
    /// COUNT(*) fast path. Returns the local replica's row count
    /// for `[start, end]` on `table_id`. Bypasses the streaming
    /// range-read RPC entirely — calls `StorageEngine::count_range`
    /// which uses the metadata-only merger
    /// (`range_merger::merger_for_metadata_sources`) so cell
    /// payloads are byte-skipped at every SSTable.
    ///
    /// Consistency: returns the LOCAL replica's view. For
    /// quorum / all consistency on COUNT(*), shipping partition
    /// keys across replicas would defeat the optimization — that
    /// is a separate design (and matches Cassandra's "COUNT is
    /// eventually consistent by default" semantics).
    pub fn coordinate_range_count(
        &self,
        table_id: &TableId,
    ) -> crate::error::Result<u64> {
        self.storage
            .count_range(table_id, None, None)
            .map_err(ClusterError::Storage)
    }
}

/// Deduplicate partitions by token — multiple replicas (RF=N) each
/// return a copy of every partition they own; without this, COUNT(*)
/// and full-table scans return N× the real partition count. Mirrors
/// the dedup loop at the end of `coordinate_range_read_limited_rows`
/// in `read.rs` so the streaming and legacy paths return the same
/// shape to the CQL layer.
fn dedup_by_token(partitions: Vec<Partition>) -> Vec<Partition> {
    let mut by_token: BTreeMap<i64, Vec<Partition>> = BTreeMap::new();
    for p in partitions {
        by_token.entry(p.key.token.0).or_default().push(p);
    }
    by_token
        .into_values()
        .map(|group| {
            if group.len() == 1 {
                group.into_iter().next().unwrap()
            } else {
                ferrosa_storage::merge::merge_partitions(group)
            }
        })
        .collect()
}

impl ClusterCoordinator {
    /// ADR-020 streaming range-read entry point.
    ///
    /// Registers a per-call route on the shared `StreamRouter`,
    /// fires a `RangeReadStreamRequest` to every remote replica,
    /// reads the local replica directly, and consumes the streamed
    /// chunks under the idle-timeout watchdog. Always unregisters
    /// the route on exit (success or error) so the routing table
    /// does not leak.
    ///
    /// `limit` and `row_limit` are passed through to the local read
    /// for parity with the legacy method but are not enforced on
    /// remote replicas in Phase 1 — the remote handler reads via
    /// the existing storage path which already caps at 10K
    /// partitions per replica. Phase 2's lazy storage iterator
    /// removes the cap and adds an explicit max-partitions hint to
    /// the request payload.
    pub async fn coordinate_range_read_stream_limited_rows(
        &self,
        table_id: &TableId,
        limit: usize,
        row_limit: usize,
    ) -> crate::error::Result<Vec<Partition>> {
        let limit = limit.clamp(1, crate::write_path::DEFAULT_RANGE_READ_LIMIT);

        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let local_id = self.local_node_id;
        let remotes: Vec<(uuid::Uuid, String)> = nodes
            .iter()
            .filter(|(id, _)| *id != local_id)
            .filter_map(|(_, host)| host.clone())
            .collect();
        let expected_done = remotes.len();

        // Local read goes direct — no internode hop.
        let mut all_partitions = match read_local_range_limited_rows(
            self.storage.as_ref(),
            table_id,
            limit,
            row_limit,
        ) {
            Ok(ps) => ps,
            Err(e) => return Err(ClusterError::Storage(e)),
        };

        // No remote replicas → done after the local read.
        if expected_done == 0 {
            return Ok(all_partitions);
        }

        let request_id = self.next_stream_request_id();
        let receiver = self
            .stream_router
            .register(request_id, STREAM_RECEIVER_BUFFER);

        // Fire RangeReadStreamRequest to every remote replica.
        let req_payload = RangeReadStreamRequestPayload {
            request_id,
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
        };
        let req_body = Bytes::from(
            bincode::serialize(&req_payload).map_err(|e| {
                ClusterError::Internal(format!("streaming range read: encode request: {e}"))
            })?,
        );

        let mut fire_failures: Vec<(uuid::Uuid, String)> = Vec::new();
        for (host_id, _addr) in &remotes {
            if let Err(e) = self
                .peer_manager
                .fire(
                    *host_id,
                    Message::RangeReadStreamRequest(req_body.clone()),
                    Lane::Bulk,
                )
                .await
            {
                tracing::warn!(
                    request_id,
                    peer = %host_id,
                    "streaming range read: failed to fire request: {e}"
                );
                fire_failures.push((*host_id, e.to_string()));
            }
        }

        // If every fire failed, no Done will ever arrive — bail out
        // immediately rather than hanging on the watchdog.
        if fire_failures.len() == expected_done {
            self.stream_router.unregister(request_id);
            return Err(ClusterError::Internal(format!(
                "streaming range read: every replica fire failed ({fire_failures:?})"
            )));
        }

        // Consume only the replicas we successfully fired to.
        let live_remote_count = expected_done - fire_failures.len();
        let consume_result = consume_range_stream(
            receiver,
            STREAMING_IDLE_TIMEOUT,
            live_remote_count,
            request_id,
        )
        .await;

        // Always unregister so the routing table doesn't leak.
        self.stream_router.unregister(request_id);

        match consume_result {
            Ok(outcome) => {
                all_partitions.extend(outcome.partitions);
                if !fire_failures.is_empty() {
                    tracing::warn!(
                        request_id,
                        failed = fire_failures.len(),
                        succeeded = live_remote_count,
                        "streaming range read: partial — some replicas could not be reached"
                    );
                }
                Ok(dedup_by_token(all_partitions))
            }
            Err(StreamConsumeError::IdleTimeout { idle_timeout, .. }) => {
                Err(ClusterError::Internal(format!(
                    "streaming range read: idle timeout after {idle_timeout:?}"
                )))
            }
            Err(StreamConsumeError::ChannelClosedBeforeDone {
                delivered_done,
                expected_done,
            }) => Err(ClusterError::Internal(format!(
                "streaming range read: channel closed after {delivered_done}/{expected_done} Done frames"
            ))),
            Err(e) => Err(ClusterError::Internal(format!(
                "streaming range read: {e:?}"
            ))),
        }
    }
}
