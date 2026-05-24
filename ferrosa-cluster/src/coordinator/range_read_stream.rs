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
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;
use futures::{Stream, StreamExt};

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

pub type ClusterPartitionStream =
    Pin<Box<dyn Stream<Item = crate::error::Result<Partition>> + Send>>;

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
    pub fn coordinate_range_count(&self, table_id: &TableId) -> crate::error::Result<u64> {
        self.storage
            .count_range(table_id, None, None)
            .map_err(ClusterError::Storage)
    }
}

/// Number of REMOTE replicas to query for a range read at the given
/// consistency level. Local replica always reads directly (counts as
/// one satisfied response), so this returns the *additional* remote
/// count needed.
///
/// For RF == node_count (every node owns every token range, typical
/// in the dev/test cluster), CL=ONE / LOCAL_ONE is satisfied by the
/// local read alone and we return 0 — full fan-out is wasted work
/// since dedup would just collapse identical replica copies anyway.
///
/// For RF < node_count we cannot prove the local node owns every
/// partition without a token-range-aware query plan; conservatively
/// fall back to the existing all-remotes fan-out until that proper
/// path lands. (Filed as next-step in
/// bug-streaming-range-read-perf-50x-floor.md.)
fn remote_count_for_cl(
    cl: crate::consistency::ConsistencyLevel,
    rf: usize,
    node_count: usize,
    remote_count: usize,
) -> usize {
    use crate::consistency::ConsistencyLevel as CL;
    // RF<cluster — fall back to full fan-out for correctness.
    if rf < node_count {
        return remote_count;
    }
    // RF==cluster — local has every partition. Apply CL.
    match cl {
        CL::One | CL::LocalOne => 0,
        // QUORUM/ALL: contact enough remotes to satisfy CL beyond
        // the local response. For RF=N, QUORUM = floor(N/2)+1.
        CL::Quorum | CL::LocalQuorum | CL::EachQuorum => {
            let needed = rf / 2 + 1; // QUORUM count including local
            needed.saturating_sub(1).min(remote_count)
        }
        CL::All => remote_count,
        // Any/Two/Three are unusual or write-only — keep
        // conservative full fan-out so we never under-read.
        _ => remote_count,
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

async fn read_local_range_stream_limited_rows(
    storage: &ferrosa_storage::StorageEngine,
    table_id: &TableId,
    limit: usize,
    row_limit: usize,
) -> ferrosa_common::Result<Vec<Partition>> {
    let mut stream = storage.range_iter(table_id, None, None);
    let mut partitions = Vec::with_capacity(limit);

    while partitions.len() < limit {
        let Some(next) = stream.next().await else {
            break;
        };
        let mut partition = next?;
        if row_limit > 0 {
            partition.rows.truncate(row_limit);
        }
        partitions.push(partition);
    }

    Ok(partitions)
}

impl ClusterCoordinator {
    /// Uncapped streaming range-read entry point.
    ///
    /// This is used by full-table CQL scans whose result must be complete
    /// (`ALLOW FILTERING`, `SELECT DISTINCT`, and uncapped `SELECT *`). The
    /// legacy materializing range RPC is intentionally not used here because it
    /// applies `DEFAULT_RANGE_READ_LIMIT` and would silently return partial
    /// query results.
    pub async fn coordinate_range_read_stream_all(
        &self,
        table_id: &TableId,
        row_limit: usize,
    ) -> crate::error::Result<ClusterPartitionStream> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let nodes: Vec<(u64, Option<(uuid::Uuid, String)>)> = node_ids
            .iter()
            .map(|&id| (id, ring.get_node(id).map(|n| (n.host_id, n.addr.clone()))))
            .collect();
        drop(ring);

        let local_id = self.local_node_id;
        let node_count = nodes.len();
        let all_remotes: Vec<(uuid::Uuid, String)> = nodes
            .iter()
            .filter(|(id, _)| *id != local_id)
            .filter_map(|(_, host)| host.clone())
            .collect();
        let cl_remote_count = remote_count_for_cl(
            self.default_cl,
            self.default_rf,
            node_count,
            all_remotes.len(),
        );
        let remotes: Vec<(uuid::Uuid, String)> =
            all_remotes.into_iter().take(cl_remote_count).collect();
        let expected_done = remotes.len();

        if expected_done > 0 {
            return Err(ClusterError::Internal(
                "unbounded cluster range scan would require replica merge/dedup across remote streams; refusing to materialize full results".into(),
            ));
        }

        let stream = self
            .storage
            .range_iter(table_id, None, None)
            .map(move |item| {
                let mut partition = item.map_err(ClusterError::Storage)?;
                if row_limit > 0 {
                    partition.rows.truncate(row_limit);
                }
                Ok(partition)
            });
        Ok(Box::pin(stream))
    }

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
        let node_count = nodes.len();
        let all_remotes: Vec<(uuid::Uuid, String)> = nodes
            .iter()
            .filter(|(id, _)| *id != local_id)
            .filter_map(|(_, host)| host.clone())
            .collect();

        // CL-aware fan-out. The local replica counts as one
        // satisfied response, so we only need to contact
        // additional remotes when the configured consistency
        // demands more than one response AND the local node
        // doesn't already own every token range.
        //
        // RF=cluster_size case (every node owns every partition,
        // typical for the test cluster): CL=ONE / LOCAL_ONE is
        // satisfied entirely by the local read.
        // RF<cluster_size case: token-ownership-aware fan-out is
        // required for correctness — we conservatively fall back
        // to the full fan-out for those tables until the proper
        // per-token-range query path lands.
        let cl_remote_count = remote_count_for_cl(
            self.default_cl,
            self.default_rf,
            node_count,
            all_remotes.len(),
        );
        let remotes: Vec<(uuid::Uuid, String)> =
            all_remotes.into_iter().take(cl_remote_count).collect();
        let expected_done = remotes.len();

        // Local read goes direct — no internode hop.
        let mut all_partitions = match read_local_range_stream_limited_rows(
            self.storage.as_ref(),
            table_id,
            limit,
            row_limit,
        )
        .await
        {
            Ok(ps) => ps,
            Err(e) => return Err(ClusterError::Storage(e)),
        };

        // No remote replicas → done after the local read.
        if expected_done == 0 {
            return Ok(dedup_by_token(all_partitions));
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
        let req_body = Bytes::from(bincode::serialize(&req_payload).map_err(|e| {
            ClusterError::Internal(format!("streaming range read: encode request: {e}"))
        })?);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consistency::ConsistencyLevel as CL;

    /// RF=cluster_size: local replica owns every partition, so
    /// CL=ONE / LOCAL_ONE needs zero remote replicas. QUORUM needs
    /// floor(RF/2)+1 total responses → that count minus 1 (local)
    /// is the remote count. ALL needs every remote.
    #[test]
    fn cl_remote_count_rf_equals_cluster() {
        // 3 nodes, RF=3, 2 remotes.
        assert_eq!(remote_count_for_cl(CL::One, 3, 3, 2), 0);
        assert_eq!(remote_count_for_cl(CL::LocalOne, 3, 3, 2), 0);
        assert_eq!(remote_count_for_cl(CL::Quorum, 3, 3, 2), 1);
        assert_eq!(remote_count_for_cl(CL::LocalQuorum, 3, 3, 2), 1);
        assert_eq!(remote_count_for_cl(CL::All, 3, 3, 2), 2);
        // 5 nodes, RF=5, 4 remotes.
        assert_eq!(remote_count_for_cl(CL::One, 5, 5, 4), 0);
        assert_eq!(remote_count_for_cl(CL::Quorum, 5, 5, 4), 2);
        assert_eq!(remote_count_for_cl(CL::All, 5, 5, 4), 4);
    }

    /// RF<cluster_size: we cannot prove the local node owns every
    /// token range, so we fall back to the full fan-out for
    /// correctness — under-reading would surface as missing
    /// partitions for whichever ranges the local node doesn't own.
    /// Replace with a token-aware query plan in a follow-up.
    #[test]
    fn cl_remote_count_rf_less_than_cluster_falls_back_to_full_fanout() {
        // 5 nodes, RF=3 — local may not own every range.
        assert_eq!(remote_count_for_cl(CL::One, 3, 5, 4), 4);
        assert_eq!(remote_count_for_cl(CL::Quorum, 3, 5, 4), 4);
        assert_eq!(remote_count_for_cl(CL::All, 3, 5, 4), 4);
    }

    #[test]
    fn coordinate_streaming_range_read_does_not_call_vec_local_read() {
        let source = include_str!("range_read_stream.rs");
        let body = source
            .split("pub async fn coordinate_range_read_stream_limited_rows")
            .nth(1)
            .and_then(|rest| rest.split("#[cfg(test)]").next())
            .expect("streaming coordinator body must be present");

        assert!(
            !body.contains("read_local_range_limited_rows"),
            "streaming range coordinator must not call the Vec-returning local read helper: {body}"
        );
        assert!(
            body.contains("read_local_range_stream_limited_rows"),
            "streaming range coordinator must route local reads through the bounded streaming helper: {body}"
        );
        let helper = source
            .split("async fn read_local_range_stream_limited_rows")
            .nth(1)
            .and_then(|rest| rest.split("impl ClusterCoordinator").next())
            .expect("streaming local read helper must be present");
        assert!(
            helper.contains("range_iter") && helper.contains("while partitions.len() < limit"),
            "streaming local read helper must pull from range_iter under the requested limit: {helper}"
        );
    }

    #[test]
    fn unbounded_streaming_range_read_boundary_must_not_return_vec() {
        let source = include_str!("range_read_stream.rs");
        let body = source
            .split("pub async fn coordinate_range_read_stream_all")
            .nth(1)
            .and_then(|rest| {
                rest.split("pub async fn coordinate_range_read_stream_limited_rows")
                    .next()
            })
            .expect("unbounded streaming range-read body must be present");

        assert!(
            !body.contains("Result<Vec<Partition>>"),
            "unbounded streaming range reads must expose a partition stream, not materialize Vec<Partition>"
        );
        assert!(
            !body.contains("let mut all_partitions"),
            "unbounded streaming range reads must not accumulate local and remote partitions before returning"
        );
        assert!(
            body.contains("refusing to materialize full results"),
            "remote unbounded scans that need replica merge must fail clearly instead of falling back to materialization"
        );
    }
}
