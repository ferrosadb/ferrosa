//! Consumer for streaming range-read responses (ADR-020).
//!
//! Receives the multi-message stream coming back from one or more
//! replicas — `RangeReadStreamChunk`, `RangeReadStreamHeartbeat`,
//! and `RangeReadStreamDone` frames keyed by the same `request_id`
//! — and assembles them into a flat `Vec<Partition>`.
//!
//! Time-bounded by `IdleTimeoutWatchdog`: total wall-clock can be
//! unbounded (PB-scale scans take hours), but a producer that stops
//! sending entirely for longer than `idle_timeout` aborts the
//! consume with [`StreamConsumeError::IdleTimeout`]. Heartbeats from
//! the producer count as activity and reset the deadline alongside
//! chunks.
//!
//! This module is the pure consumption logic — no storage, no
//! transport. The handler that produces chunks and the coordinator
//! method that wires up routes, replicas, and deduplication live one
//! layer up.

use std::time::Duration;

use bytes::Bytes;
use ferrosa_net::codec::MsgType;
use ferrosa_net::idle_timeout::IdleTimeoutWatchdog;
use ferrosa_net::message::Message;
use ferrosa_sstable::types::Partition;
use tokio::sync::mpsc;

use crate::raft::handlers::{
    partition_from_wire, RangeReadStreamChunkPayload, RangeReadStreamDonePayload,
    RangeReadStreamHeartbeatPayload,
};

/// What the consumer collected once every replica's stream
/// terminated cleanly.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StreamConsumeOutcome {
    /// Partitions aggregated across all replicas, in arrival order.
    /// Deduplication and merge happen at a higher layer once all
    /// replica streams have completed.
    pub partitions: Vec<Partition>,
    /// Total number of chunk frames observed across all replicas.
    pub total_chunks: u32,
    /// `true` if at least one replica's `Done` reported `truncated`.
    pub any_truncated: bool,
}

/// All the ways consuming a streaming range read can fail. The
/// underlying `request_id` is included whenever it is unambiguously
/// known so log lines correlate with the producer side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamConsumeError {
    /// The producer stopped sending entirely for longer than the
    /// configured `idle_timeout` — i.e., no chunk, no heartbeat, no
    /// done frame within the deadline.
    IdleTimeout {
        request_id: u32,
        idle_timeout: Duration,
    },
    /// A chunk, heartbeat, or done payload failed bincode decode.
    /// Indicates protocol corruption or a peer running an
    /// incompatible payload shape.
    Decode {
        request_id: u32,
        which: &'static str,
        message: String,
    },
    /// The receiver yielded a `Message` whose type does not belong on
    /// a streaming range-read response. Indicates a routing bug.
    UnexpectedFrame { msg_type: MsgType },
    /// The receiver was closed (all senders dropped) before every
    /// expected `Done` frame arrived. Surfaces as a partial result on
    /// the caller side.
    ChannelClosedBeforeDone {
        delivered_done: usize,
        expected_done: usize,
    },
}

/// Consume a streaming range-read response.
///
/// `receiver` is the per-request channel produced by
/// `StreamRouter::register`. `idle_timeout` bounds inactivity on the
/// stream (passed through `IdleTimeoutWatchdog`). `expected_done` is
/// the number of replicas whose streams must terminate cleanly
/// before the consume returns — one `Done` per replica.
/// `request_id` is included in error variants for correlation.
pub async fn consume_range_stream(
    receiver: mpsc::Receiver<Message>,
    idle_timeout: Duration,
    expected_done: usize,
    request_id: u32,
) -> Result<StreamConsumeOutcome, StreamConsumeError> {
    let mut watchdog = IdleTimeoutWatchdog::new(receiver, idle_timeout);
    let mut outcome = StreamConsumeOutcome::default();
    let mut observed_chunks_per_replica: u32 = 0;
    let mut delivered_done = 0usize;

    loop {
        if delivered_done >= expected_done {
            // All Dones received. The server spawns a task per
            // inbound frame so Chunk handlers can race the Done
            // handler — drain any straggler chunks that arrived
            // after the Done was routed but before this loop saw
            // them. Bounded by a short grace period; long-tail
            // chunks beyond the window are lost (logged at debug).
            drain_stragglers(&mut watchdog, &mut outcome, request_id).await;
            return Ok(outcome);
        }

        let next = watchdog
            .next()
            .await
            .map_err(|elapsed| StreamConsumeError::IdleTimeout {
                request_id,
                idle_timeout: elapsed.idle_timeout,
            })?;

        let frame = match next {
            Some(msg) => msg,
            None => {
                return Err(StreamConsumeError::ChannelClosedBeforeDone {
                    delivered_done,
                    expected_done,
                });
            }
        };

        match frame {
            Message::RangeReadStreamChunk(bytes) => {
                let chunk = decode_chunk(request_id, bytes)?;
                outcome.total_chunks = outcome.total_chunks.saturating_add(1);
                observed_chunks_per_replica = observed_chunks_per_replica.saturating_add(1);
                outcome
                    .partitions
                    .extend(chunk.partitions.into_iter().map(partition_from_wire));
            }
            Message::RangeReadStreamHeartbeat(bytes) => {
                // Drop the payload after a successful decode — we
                // only ever care about heartbeats as evidence of
                // activity (the watchdog already reset).
                let _heartbeat = decode_heartbeat(request_id, bytes)?;
            }
            Message::RangeReadStreamDone(bytes) => {
                let done = decode_done(request_id, bytes)?;
                // Chunk counts disagreeing with Done is observable but
                // expected under the current server: ferrosa-net's
                // rpc::server::run_connection spawns a tokio task per
                // inbound frame so handlers for Chunk vs Done race —
                // Done can be routed onto the per-request mpsc before
                // a preceding Chunk's handler runs. We do not treat
                // this as an error; total_chunks is telemetry. After
                // the Done count is satisfied below we drain remaining
                // chunks with a short grace period so stragglers land
                // in the result instead of being lost to the unregister
                // call.
                if done.total_chunks != observed_chunks_per_replica {
                    tracing::debug!(
                        request_id,
                        reported = done.total_chunks,
                        observed = observed_chunks_per_replica,
                        "stream Done arrived before all preceding chunks were routed; will drain stragglers"
                    );
                }
                if done.truncated {
                    outcome.any_truncated = true;
                }
                observed_chunks_per_replica = 0;
                delivered_done += 1;
            }
            other => {
                return Err(StreamConsumeError::UnexpectedFrame {
                    msg_type: other.msg_type(),
                });
            }
        }
    }
}

/// Grace period for draining stragglers after the last Done.
/// Sized to absorb tokio-task scheduling jitter between Chunk and
/// Done dispatches on the server side (typically sub-millisecond).
const STRAGGLER_DRAIN: Duration = Duration::from_millis(200);

async fn drain_stragglers(
    watchdog: &mut IdleTimeoutWatchdog<Message>,
    outcome: &mut StreamConsumeOutcome,
    request_id: u32,
) {
    let deadline = tokio::time::Instant::now() + STRAGGLER_DRAIN;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let next = match tokio::time::timeout(remaining, watchdog.next()).await {
            Ok(Ok(Some(msg))) => msg,
            // Stream closed cleanly, watchdog tripped, or grace
            // window elapsed — all terminate the drain.
            Ok(Ok(None)) | Ok(Err(_)) | Err(_) => break,
        };
        match next {
            Message::RangeReadStreamChunk(bytes) => match decode_chunk(request_id, bytes) {
                Ok(chunk) => {
                    outcome.total_chunks = outcome.total_chunks.saturating_add(1);
                    outcome
                        .partitions
                        .extend(chunk.partitions.into_iter().map(partition_from_wire));
                }
                Err(e) => tracing::debug!(?e, "straggler chunk decode failed"),
            },
            // Stragglers we don't care about.
            Message::RangeReadStreamHeartbeat(_) | Message::RangeReadStreamDone(_) => {}
            other => tracing::debug!(?other, "straggler frame of unexpected type; dropped"),
        }
    }
}

fn decode_chunk(
    request_id: u32,
    bytes: Bytes,
) -> Result<RangeReadStreamChunkPayload, StreamConsumeError> {
    bincode::deserialize::<RangeReadStreamChunkPayload>(&bytes).map_err(|e| {
        StreamConsumeError::Decode {
            request_id,
            which: "RangeReadStreamChunk",
            message: e.to_string(),
        }
    })
}

fn decode_heartbeat(
    request_id: u32,
    bytes: Bytes,
) -> Result<RangeReadStreamHeartbeatPayload, StreamConsumeError> {
    bincode::deserialize::<RangeReadStreamHeartbeatPayload>(&bytes).map_err(|e| {
        StreamConsumeError::Decode {
            request_id,
            which: "RangeReadStreamHeartbeat",
            message: e.to_string(),
        }
    })
}

fn decode_done(
    request_id: u32,
    bytes: Bytes,
) -> Result<RangeReadStreamDonePayload, StreamConsumeError> {
    bincode::deserialize::<RangeReadStreamDonePayload>(&bytes).map_err(|e| {
        StreamConsumeError::Decode {
            request_id,
            which: "RangeReadStreamDone",
            message: e.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, Partition};

    use crate::raft::handlers::partition_to_wire;

    const REQ_ID: u32 = 42;
    const IDLE: Duration = Duration::from_millis(500);

    fn make_partition(tag: u8) -> Partition {
        let key = DecoratedKey::new(PartitionKey::new(vec![tag]));
        Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![],
        }
    }

    fn chunk_msg(seq: u32, partitions: Vec<Partition>) -> Message {
        let payload = RangeReadStreamChunkPayload {
            request_id: REQ_ID,
            seq,
            partitions: partitions.into_iter().map(partition_to_wire).collect(),
        };
        Message::RangeReadStreamChunk(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    fn heartbeat_msg(seq: u32) -> Message {
        let payload = RangeReadStreamHeartbeatPayload {
            request_id: REQ_ID,
            seq,
        };
        Message::RangeReadStreamHeartbeat(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    fn done_msg(total_chunks: u32, truncated: bool) -> Message {
        let payload = RangeReadStreamDonePayload {
            request_id: REQ_ID,
            total_chunks,
            truncated,
        };
        Message::RangeReadStreamDone(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    /// A complete single-replica stream of N chunks + 1 done frame
    /// resolves to the flat partition list.
    #[tokio::test]
    async fn single_replica_chunks_and_done_assemble_partitions_in_order() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(chunk_msg(0, vec![make_partition(1), make_partition(2)]))
            .await
            .unwrap();
        tx.send(chunk_msg(1, vec![make_partition(3)]))
            .await
            .unwrap();
        tx.send(done_msg(2, false)).await.unwrap();

        let outcome = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap();
        assert_eq!(outcome.total_chunks, 2);
        assert!(!outcome.any_truncated);
        assert_eq!(outcome.partitions.len(), 3);
        assert_eq!(outcome.partitions[0].key.key.as_bytes(), b"\x01");
        assert_eq!(outcome.partitions[1].key.key.as_bytes(), b"\x02");
        assert_eq!(outcome.partitions[2].key.key.as_bytes(), b"\x03");
    }

    /// Heartbeats between chunks count as activity for the watchdog
    /// and don't appear in the assembled outcome.
    #[tokio::test]
    async fn heartbeats_keep_stream_alive_and_are_not_counted_as_chunks() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(heartbeat_msg(0)).await.unwrap();
        tx.send(chunk_msg(0, vec![make_partition(1)]))
            .await
            .unwrap();
        tx.send(heartbeat_msg(1)).await.unwrap();
        tx.send(done_msg(1, false)).await.unwrap();

        let outcome = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap();
        assert_eq!(outcome.total_chunks, 1);
        assert_eq!(outcome.partitions.len(), 1);
    }

    /// Multiple replicas: stream completes when `expected_done` Done
    /// frames have arrived, partitions accumulate across both.
    #[tokio::test]
    async fn two_replicas_done_only_when_both_terminate() {
        let (tx, rx) = mpsc::channel(8);
        // Replica A: 1 chunk + done. Replica B: 1 chunk + done.
        // Interleaved on the same routed channel.
        tx.send(chunk_msg(0, vec![make_partition(0xA)]))
            .await
            .unwrap();
        tx.send(chunk_msg(0, vec![make_partition(0xB)]))
            .await
            .unwrap();
        tx.send(done_msg(2, false)).await.unwrap();
        // First Done consumed → still waiting for the second.
        tx.send(done_msg(0, false)).await.unwrap();

        // expected_done = 2.
        let outcome = consume_range_stream(rx, IDLE, 2, REQ_ID).await.unwrap();
        assert_eq!(outcome.partitions.len(), 2);
        assert_eq!(outcome.total_chunks, 2);
    }

    /// Producer stops sending entirely → watchdog trips with
    /// `IdleTimeout` carrying the configured duration and request_id.
    #[tokio::test(start_paused = true)]
    async fn stalled_producer_surfaces_idle_timeout() {
        let (_tx, rx) = mpsc::channel(8);
        let err = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap_err();
        assert_eq!(
            err,
            StreamConsumeError::IdleTimeout {
                request_id: REQ_ID,
                idle_timeout: IDLE,
            }
        );
    }

    /// Channel closes before Done arrives → ChannelClosedBeforeDone
    /// reports how many Done frames were observed.
    #[tokio::test]
    async fn channel_close_before_done_surfaces_partial() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(chunk_msg(0, vec![make_partition(1)]))
            .await
            .unwrap();
        drop(tx); // no Done

        let err = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap_err();
        assert_eq!(
            err,
            StreamConsumeError::ChannelClosedBeforeDone {
                delivered_done: 0,
                expected_done: 1,
            }
        );
    }

    /// A message that doesn't belong to the streaming range-read
    /// response set is a routing bug and surfaces as
    /// `UnexpectedFrame`.
    #[tokio::test]
    async fn unexpected_message_type_is_reported() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(Message::Ping {
            nonce: 0,
            sent_at: 0,
        })
        .await
        .unwrap();

        let err = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap_err();
        assert_eq!(
            err,
            StreamConsumeError::UnexpectedFrame {
                msg_type: MsgType::Ping
            }
        );
    }

    /// Done arriving with a `total_chunks` count that disagrees
    /// with what the consumer observed is tolerated — the server
    /// spawns a task per inbound frame, so Done can be routed
    /// onto the per-request mpsc before a preceding Chunk's
    /// handler runs. The consumer drains stragglers within a short
    /// grace window so the missing chunk lands in the result, and
    /// the mismatch is logged at debug rather than surfacing as an
    /// error.
    #[tokio::test]
    async fn done_with_higher_chunk_count_drains_straggler() {
        let (tx, rx) = mpsc::channel(8);
        // Done first (race winner), Chunk after — simulates the
        // server-side Chunk-vs-Done dispatch race.
        tx.send(done_msg(1, false)).await.unwrap();
        tx.send(chunk_msg(0, vec![make_partition(1)]))
            .await
            .unwrap();
        drop(tx);

        let outcome = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap();
        // Straggler-drain picked up the chunk that arrived after Done.
        assert_eq!(outcome.partitions.len(), 1);
        assert_eq!(outcome.total_chunks, 1);
    }

    /// `any_truncated` is true if any replica's Done sets the flag.
    #[tokio::test]
    async fn truncation_flag_propagates_from_any_replica() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(done_msg(0, true)).await.unwrap();
        tx.send(done_msg(0, false)).await.unwrap();

        let outcome = consume_range_stream(rx, IDLE, 2, REQ_ID).await.unwrap();
        assert!(outcome.any_truncated);
    }
}
