//! RPC handlers for inbound bootstrap streaming messages.
//!
//! A single [`StreamHandler`] manages session state across the three-message
//! protocol (Start → Chunk → End) for both row-based and SSTable file-based
//! streaming. Sessions are tracked in a `DashMap` keyed by `session_id`.
//!
//! **Important:** `PeerManager::send()` awaits a response. Handlers MUST return
//! `Some(Message)` — returning `None` causes the sender to block until the
//! Bulk lane times out.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;

use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_storage::engine::StorageEngine;

use super::receiver::{SstableStreamReceiver, SstableStreamSession, StreamReceiver, StreamSession};
use super::{
    SstableStreamChunkPayload, SstableStreamEndPayload, SstableStreamStartPayload,
    StreamChunkPayload, StreamEndAck, StreamEndOutcome, StreamEndPayload, StreamStartPayload,
};

/// Empty ack payload — minimal response to unblock the sender.
fn ack() -> Bytes {
    Bytes::from_static(b"ok")
}

/// `StreamEnd` reply carrying the receiver's verdict as a [`StreamEndAck`].
fn stream_end_reply(session_id: u64, outcome: StreamEndOutcome) -> Message {
    let ack = StreamEndAck {
        session_id,
        outcome,
    };
    match bincode::serialize(&ack) {
        Ok(bytes) => Message::StreamEnd(Bytes::from(bytes)),
        Err(e) => {
            // A verdict that cannot be encoded must not degrade into the
            // legacy `ok`: an empty body fails the sender's verification.
            tracing::error!(session_id, "StreamEnd: failed to encode verdict: {e}");
            Message::StreamEnd(Bytes::new())
        }
    }
}

// ---------------------------------------------------------------------------
// Row-based streaming handler
// ---------------------------------------------------------------------------

/// Handles `StreamStart`, `StreamChunk`, and `StreamEnd` messages.
///
/// Maintains in-flight sessions in a concurrent map so chunks from the same
/// session are accumulated correctly even when arriving on different
/// connection threads.
pub struct StreamHandler {
    storage: Arc<StorageEngine>,
    sessions: DashMap<u64, StreamSession>,
}

impl StreamHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            sessions: DashMap::new(),
        }
    }
}

#[async_trait]
impl RpcHandler for StreamHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::StreamStart(b) => {
                let payload: StreamStartPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("StreamStart: deserialize failed: {e}"))
                    .ok()?;
                let session_id = payload.session_id;
                let session = StreamReceiver::begin(payload);
                self.sessions.insert(session_id, session);
                Some(Message::StreamStart(ack()))
            }
            Message::StreamChunk(b) => {
                let payload: StreamChunkPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("StreamChunk: deserialize failed: {e}"))
                    .ok()?;
                let session_id = payload.session_id;
                if let Some(mut session) = self.sessions.get_mut(&session_id) {
                    if let Err(e) = session.apply_chunk(payload) {
                        tracing::error!(session_id, "StreamChunk: apply failed: {e}");
                        self.sessions.remove(&session_id);
                    }
                } else {
                    tracing::warn!(
                        session_id,
                        "StreamChunk: no session found (missed StreamStart?)"
                    );
                }
                Some(Message::StreamChunk(ack()))
            }
            Message::StreamEnd(b) => {
                let payload: StreamEndPayload = match bincode::deserialize(&b) {
                    Ok(payload) => payload,
                    Err(e) => {
                        tracing::error!("StreamEnd: deserialize failed: {e}");
                        return Some(stream_end_reply(
                            0,
                            StreamEndOutcome::Rejected {
                                reason: format!("StreamEnd payload did not decode: {e}"),
                            },
                        ));
                    }
                };
                let session_id = payload.session_id;
                // The reply carries the verdict (P0-2): a sender that changes
                // membership must learn whether the data landed, not merely
                // that the message arrived.
                let outcome = if let Some((_, session)) = self.sessions.remove(&session_id) {
                    match session.finish(payload, &self.storage) {
                        Ok(result) => {
                            tracing::info!(
                                session_id,
                                applied = result.applied,
                                "stream: session complete"
                            );
                            StreamEndOutcome::Applied {
                                applied: result.applied,
                            }
                        }
                        Err(e) => {
                            tracing::error!(session_id, "StreamEnd: finish failed: {e}");
                            StreamEndOutcome::Rejected {
                                reason: e.to_string(),
                            }
                        }
                    }
                } else {
                    tracing::warn!(
                        session_id,
                        "StreamEnd: no session found (missed StreamStart?)"
                    );
                    StreamEndOutcome::Rejected {
                        reason: format!(
                            "no session {session_id} (StreamStart lost, or a chunk failed)"
                        ),
                    }
                };
                Some(stream_end_reply(session_id, outcome))
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// SSTable file-based streaming handler
// ---------------------------------------------------------------------------

/// Handles `SstableStreamStart`, `SstableStreamChunk`, and `SstableStreamEnd`.
pub struct SstableStreamHandler {
    data_dir: PathBuf,
    sessions: DashMap<u64, SstableStreamSession>,
}

impl SstableStreamHandler {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            sessions: DashMap::new(),
        }
    }
}

#[async_trait]
impl RpcHandler for SstableStreamHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::SstableStreamStart(b) => {
                let payload: SstableStreamStartPayload = bincode::deserialize(&b)
                    .map_err(|e| {
                        tracing::error!("SstableStreamStart: deserialize failed: {e}");
                    })
                    .ok()?;
                let session_id = payload.session_id;
                let dest_dir = self.data_dir.join("sstables").join(format!(
                    "{}.{}/{}",
                    payload.keyspace, payload.table, payload.sstable_id
                ));
                let session = SstableStreamReceiver::begin(payload, dest_dir);
                self.sessions.insert(session_id, session);
                Some(Message::SstableStreamStart(ack()))
            }
            Message::SstableStreamChunk(b) => {
                let payload: SstableStreamChunkPayload = bincode::deserialize(&b)
                    .map_err(|e| {
                        tracing::error!("SstableStreamChunk: deserialize failed: {e}");
                    })
                    .ok()?;
                let session_id = payload.session_id;
                if let Some(mut session) = self.sessions.get_mut(&session_id) {
                    if let Err(e) = session.apply_chunk(payload) {
                        tracing::error!(session_id, "SstableStreamChunk: apply failed: {e}");
                        self.sessions.remove(&session_id);
                    }
                } else {
                    tracing::warn!(
                        session_id,
                        "SstableStreamChunk: no session found (missed Start?)"
                    );
                }
                Some(Message::SstableStreamChunk(ack()))
            }
            Message::SstableStreamEnd(b) => {
                let payload: SstableStreamEndPayload = bincode::deserialize(&b)
                    .map_err(|e| {
                        tracing::error!("SstableStreamEnd: deserialize failed: {e}");
                    })
                    .ok()?;
                let session_id = payload.session_id;
                if let Some((_, session)) = self.sessions.remove(&session_id) {
                    match session.finish(payload) {
                        Ok(result) => {
                            tracing::info!(
                                session_id,
                                files = result.written_files.len(),
                                bytes = result.total_bytes,
                                "sstable_stream: session complete"
                            );
                        }
                        Err(e) => {
                            tracing::error!(session_id, "SstableStreamEnd: finish failed: {e}");
                        }
                    }
                } else {
                    tracing::warn!(
                        session_id,
                        "SstableStreamEnd: no session found (missed Start?)"
                    );
                }
                Some(Message::SstableStreamEnd(ack()))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::sender::verify_stream_end_reply;
    use crate::streaming::{StreamEndAck, StreamEndOutcome};

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
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
            cache_hot_window_secs: 900,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn peer() -> PeerId {
        (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap())
    }

    async fn end_session(handler: &StreamHandler, session_id: u64, checksum: u32) -> Message {
        let start = StreamStartPayload {
            session_id,
            source_node: 1,
            token_range_start: i64::MIN,
            token_range_end: i64::MAX,
            estimated_bytes: 0,
        };
        handler
            .handle(
                peer(),
                Message::StreamStart(Bytes::from(bincode::serialize(&start).unwrap())),
            )
            .await
            .expect("StreamStart reply");
        let end = StreamEndPayload {
            session_id,
            total_mutations: 0,
            checksum,
        };
        handler
            .handle(
                peer(),
                Message::StreamEnd(Bytes::from(bincode::serialize(&end).unwrap())),
            )
            .await
            .expect("StreamEnd reply")
    }

    fn decode_ack(reply: &Message) -> StreamEndAck {
        let Message::StreamEnd(bytes) = reply else {
            panic!("expected a StreamEnd reply, got {reply:?}");
        };
        bincode::deserialize(bytes).expect("StreamEnd reply must carry a StreamEndAck verdict")
    }

    /// P0-2: a session whose checksum does not match must be reported as
    /// REJECTED to the sender. The reply used to be `b"ok"` regardless, so a
    /// decommission counted a discarded stream as moved data.
    #[tokio::test]
    async fn stream_end_reply_reports_a_checksum_mismatch_as_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let handler = StreamHandler::new(test_storage(dir.path()));
        let empty_checksum = crc32fast::Hasher::new().finalize();

        let reply = end_session(&handler, 7, empty_checksum.wrapping_add(1)).await;

        let ack = decode_ack(&reply);
        assert_eq!(ack.session_id, 7);
        assert!(
            matches!(&ack.outcome, StreamEndOutcome::Rejected { reason } if reason.contains("checksum")),
            "a checksum mismatch must be rejected, got {ack:?}"
        );
        assert!(verify_stream_end_reply(&reply, 7, 0).is_err());
    }

    /// The verified path: matching count and checksum reply `Applied`.
    #[tokio::test]
    async fn stream_end_reply_reports_an_applied_session() {
        let dir = tempfile::tempdir().unwrap();
        let handler = StreamHandler::new(test_storage(dir.path()));
        let empty_checksum = crc32fast::Hasher::new().finalize();

        let reply = end_session(&handler, 9, empty_checksum).await;

        assert_eq!(
            decode_ack(&reply),
            StreamEndAck {
                session_id: 9,
                outcome: StreamEndOutcome::Applied { applied: 0 },
            }
        );
        assert_eq!(verify_stream_end_reply(&reply, 9, 0).unwrap(), 0);
    }

    /// A `StreamEnd` for a session the receiver never started (a lost
    /// `StreamStart`, or a session dropped after a failed chunk) is rejected.
    #[tokio::test]
    async fn stream_end_reply_rejects_an_unknown_session() {
        let dir = tempfile::tempdir().unwrap();
        let handler = StreamHandler::new(test_storage(dir.path()));
        let end = StreamEndPayload {
            session_id: 11,
            total_mutations: 0,
            checksum: 0,
        };
        let reply = handler
            .handle(
                peer(),
                Message::StreamEnd(Bytes::from(bincode::serialize(&end).unwrap())),
            )
            .await
            .expect("StreamEnd reply");

        assert!(matches!(
            decode_ack(&reply).outcome,
            StreamEndOutcome::Rejected { .. }
        ));
    }

    /// The sender refuses a reply with no verdict (a pre-upgrade receiver's
    /// bare `ok`), a verdict for another session, and an applied count that
    /// does not match what was sent.
    #[test]
    fn sender_refuses_unverified_or_mismatched_replies() {
        let legacy = Message::StreamEnd(Bytes::from_static(b"ok"));
        assert!(
            verify_stream_end_reply(&legacy, 1, 3).is_err(),
            "a bare ok says nothing about whether the data landed"
        );

        let ack = |session_id, applied| {
            Message::StreamEnd(Bytes::from(
                bincode::serialize(&StreamEndAck {
                    session_id,
                    outcome: StreamEndOutcome::Applied { applied },
                })
                .unwrap(),
            ))
        };
        assert!(
            verify_stream_end_reply(&ack(2, 3), 1, 3).is_err(),
            "wrong session"
        );
        assert!(
            verify_stream_end_reply(&ack(1, 2), 1, 3).is_err(),
            "short apply"
        );
        assert_eq!(verify_stream_end_reply(&ack(1, 3), 1, 3).unwrap(), 3);
    }
}
