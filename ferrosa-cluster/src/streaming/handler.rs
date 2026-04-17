//! RPC handlers for inbound bootstrap streaming messages.
//!
//! A single [`StreamHandler`] manages session state across the three-message
//! protocol (Start → Chunk → End) for both row-based and SSTable file-based
//! streaming. Sessions are tracked in a `DashMap` keyed by `session_id`.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_storage::engine::StorageEngine;

use super::receiver::{SstableStreamReceiver, SstableStreamSession, StreamReceiver, StreamSession};
use super::{
    SstableStreamChunkPayload, SstableStreamEndPayload, SstableStreamStartPayload,
    StreamChunkPayload, StreamEndPayload, StreamStartPayload,
};

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
                None // fire-and-forget
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
                None
            }
            Message::StreamEnd(b) => {
                let payload: StreamEndPayload = bincode::deserialize(&b)
                    .map_err(|e| tracing::error!("StreamEnd: deserialize failed: {e}"))
                    .ok()?;
                let session_id = payload.session_id;
                if let Some((_, session)) = self.sessions.remove(&session_id) {
                    match session.finish(payload, &self.storage) {
                        Ok(result) => {
                            tracing::info!(
                                session_id,
                                applied = result.applied,
                                "stream: session complete"
                            );
                        }
                        Err(e) => {
                            tracing::error!(session_id, "StreamEnd: finish failed: {e}");
                        }
                    }
                } else {
                    tracing::warn!(
                        session_id,
                        "StreamEnd: no session found (missed StreamStart?)"
                    );
                }
                None
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
                None
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
                None
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
                None
            }
            _ => None,
        }
    }
}
