use std::sync::Arc;

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::{CommitLogPosition, Mutation};

use crate::error::{ClusterError, Result};
use crate::pair::coordinator::{decode_mutation, encode_mutation};

/// Initiate catch-up from the secondary side.
///
/// Sends `PairCatchUp` to primary with last known position.
/// Primary replays mutations from that point forward.
pub async fn request_catchup(
    peer_manager: &PeerManager,
    peer_host_id: Uuid,
    last_position: Option<(u64, u64)>,
) -> Result<Vec<Mutation>> {
    let (segment_id, offset) = last_position.unwrap_or((0, 0));

    let wire_offset = u32::try_from(offset)
        .map_err(|_| ClusterError::Internal("catch-up offset exceeds u32::MAX".into()))?;
    let resp = peer_manager
        .send(
            peer_host_id,
            Message::PairCatchUp {
                last_segment_id: segment_id,
                last_offset: wire_offset,
            },
            Lane::Bulk,
        )
        .await
        .map_err(ClusterError::Net)?;

    match resp {
        Message::PairCatchUpResponse(body) => {
            if body.is_empty() {
                return Err(ClusterError::CatchUpRequired);
            }
            decode_catchup_response(&body)
        }
        other => Err(ClusterError::ReplicationFailed(format!(
            "expected PairCatchUpResponse, got {:?}",
            other.msg_type()
        ))),
    }
}

/// RPC handler for PairCatchUp requests (runs on primary).
pub struct PairCatchUpHandler {
    storage: Arc<StorageEngine>,
}

impl PairCatchUpHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairCatchUpHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let (segment_id, offset) = match msg {
            Message::PairCatchUp {
                last_segment_id,
                last_offset,
            } => (last_segment_id, last_offset),
            _ => return None,
        };

        let position = CommitLogPosition {
            segment_id,
            offset: u64::from(offset),
        };

        match self.storage.replay_from(position) {
            Ok(mutations) => {
                if mutations.is_empty() {
                    Some(Message::PairCatchUpResponse(Bytes::new()))
                } else {
                    match encode_catchup_response(&mutations) {
                        Ok(body) => Some(Message::PairCatchUpResponse(body)),
                        Err(e) => {
                            tracing::error!("failed to encode catch-up response: {e}");
                            None
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("catch-up replay failed: {e}");
                Some(Message::PairCatchUpResponse(Bytes::new()))
            }
        }
    }
}

/// Largest digest a peer will build before telling the primary to send its
/// whole log instead. 65 536 ids is 1 MiB of mutation ids on the wire, which
/// stays inside the pair lane's frame limit while cutting a rejoin's payload
/// by the size of the mutations themselves (hundreds of bytes each).
pub const DEFAULT_DIGEST_LIMIT: usize = 65_536;

/// Status byte in a digest reply: the id list is complete.
const DIGEST_STATUS_COMPLETE: u8 = 0;
/// Status byte: the peer's log is larger than the requested limit.
const DIGEST_STATUS_TOO_LARGE: u8 = 1;
/// Status byte: the peer could not read its own log.
const DIGEST_STATUS_UNAVAILABLE: u8 = 2;

/// Ask a rejoining peer which mutations its own commit log already holds.
///
/// Returns `Ok(Some(ids))` when the peer produced a complete list, and
/// `Ok(None)` when it declined (log too large, unreadable, or an older build
/// with no handler) — in which case the caller replays everything, which is
/// always correct, just larger.
pub async fn request_log_digest(
    peer_manager: &PeerManager,
    peer_host_id: Uuid,
    limit: usize,
) -> Result<Option<std::collections::HashSet<[u8; 16]>>> {
    let wire_limit = u32::try_from(limit).unwrap_or(u32::MAX);
    let mut body = Vec::with_capacity(4);
    body.extend_from_slice(&wire_limit.to_be_bytes());

    let resp = peer_manager
        .send_with_timeout(
            peer_host_id,
            Message::PairLogDigest(Bytes::from(body)),
            Lane::Bulk,
            // Short: this runs inside the rejoin gate, and a peer that cannot
            // answer a control message quickly is better served by the full
            // replay than by holding a switchover off for seconds.
            std::time::Duration::from_secs(2),
        )
        .await
        .map_err(ClusterError::Net)?;

    match resp {
        Message::PairLogDigestResponse(body) => decode_digest_response(&body),
        other => Ok({
            tracing::debug!(
                response = ?other.msg_type(),
                "peer answered a log digest request with another message; replaying the full log"
            );
            None
        }),
    }
}

/// RPC handler for digest requests: the peer reads its own commit log and
/// reports the mutation ids it holds.
pub struct PairLogDigestHandler {
    storage: Arc<StorageEngine>,
}

impl PairLogDigestHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairLogDigestHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let limit = match msg {
            Message::PairLogDigest(body) => decode_digest_limit(&body),
            _ => return None,
        };

        let position = CommitLogPosition {
            segment_id: 0,
            offset: 0,
        };
        // A node's own log is what it can serve after a restart: everything in
        // it was applied to disk, so any mutation the primary finds here does
        // not need to cross the wire again.
        let mutations = match self.storage.replay_from(position) {
            Ok(mutations) => mutations,
            Err(e) => {
                tracing::warn!(%e, "log digest: replay_from failed; peer will replay in full");
                return Some(Message::PairLogDigestResponse(Bytes::from(vec![
                    DIGEST_STATUS_UNAVAILABLE,
                ])));
            }
        };

        if mutations.len() > limit {
            tracing::info!(
                mutations = mutations.len(),
                limit,
                "log digest declined: local log exceeds the peer's limit; full replay follows"
            );
            return Some(Message::PairLogDigestResponse(Bytes::from(vec![
                DIGEST_STATUS_TOO_LARGE,
            ])));
        }

        let ids: std::collections::HashSet<[u8; 16]> =
            mutations.into_iter().map(|m| m.mutation_id).collect();
        Some(Message::PairLogDigestResponse(encode_digest_response(&ids)))
    }
}

fn decode_digest_limit(body: &[u8]) -> usize {
    if body.len() < 4 {
        return 0;
    }
    let limit = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    if limit == 0 {
        DEFAULT_DIGEST_LIMIT
    } else {
        limit
    }
}

/// Format: [status:u8] [count:u32] [id:16 bytes]*
fn encode_digest_response(ids: &std::collections::HashSet<[u8; 16]>) -> Bytes {
    let count = u32::try_from(ids.len()).unwrap_or(u32::MAX);
    let mut buf = Vec::with_capacity(5 + ids.len() * 16);
    buf.push(DIGEST_STATUS_COMPLETE);
    buf.extend_from_slice(&count.to_be_bytes());
    for id in ids {
        buf.extend_from_slice(id);
    }
    Bytes::from(buf)
}

/// `Ok(None)` means "no complete list; replay everything".
fn decode_digest_response(body: &[u8]) -> Result<Option<std::collections::HashSet<[u8; 16]>>> {
    let Some(status) = body.first().copied() else {
        return Ok(None);
    };
    if status != DIGEST_STATUS_COMPLETE {
        return Ok(None);
    }
    if body.len() < 5 {
        return Err(ClusterError::Internal("truncated log digest".into()));
    }
    let count = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    if body.len() != 5 + count * 16 {
        return Err(ClusterError::Internal(format!(
            "log digest length mismatch: {count} ids in {} bytes",
            body.len()
        )));
    }
    let mut ids = std::collections::HashSet::with_capacity(count);
    let (chunks, _rest) = body[5..].as_chunks::<16>();
    for chunk in chunks {
        ids.insert(*chunk);
    }
    Ok(Some(ids))
}

/// Encode a list of mutations for catch-up response.
/// Format: [count:u32] [size:u32 data:bytes]*
fn encode_catchup_response(mutations: &[Mutation]) -> Result<Bytes> {
    let mut buf = Vec::new();
    let count = u32::try_from(mutations.len())
        .map_err(|_| ClusterError::Internal("too many mutations".into()))?;
    buf.extend_from_slice(&count.to_be_bytes());

    for mutation in mutations {
        let encoded = encode_mutation(mutation);
        let size = u32::try_from(encoded.len())
            .map_err(|_| ClusterError::Internal("mutation too large".into()))?;
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(&encoded);
    }

    Ok(Bytes::from(buf))
}

/// Decode a catch-up response into a list of mutations.
fn decode_catchup_response(body: &[u8]) -> Result<Vec<Mutation>> {
    if body.len() < 4 {
        return Err(ClusterError::Internal("truncated catch-up response".into()));
    }
    let count = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let mut offset = 4;
    let mut mutations = Vec::with_capacity(count);

    for _ in 0..count {
        if offset + 4 > body.len() {
            return Err(ClusterError::Internal("truncated mutation size".into()));
        }
        let size = u32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + size > body.len() {
            return Err(ClusterError::Internal("truncated mutation body".into()));
        }
        let mutation = decode_mutation(&body[offset..offset + size])?;
        mutations.push(mutation);
        offset += size;
    }

    Ok(mutations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_mutation(ts: i64) -> Mutation {
        let key = DecoratedKey {
            token: Token(ts),
            key: PartitionKey::new(vec![1, 2, 3]),
        };
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(vec![ts as u8], ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        Mutation {
            mutation_id: [0x81u8; 16],
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key,
            rows: vec![Arc::new(row)],
            timestamp: ts,
        }
    }

    #[test]
    fn encode_decode_catchup_response_roundtrip() {
        let mutations = vec![test_mutation(1), test_mutation(2), test_mutation(3)];
        let encoded = encode_catchup_response(&mutations).unwrap();
        let decoded = decode_catchup_response(&encoded).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].timestamp, 1);
        assert_eq!(decoded[1].timestamp, 2);
        assert_eq!(decoded[2].timestamp, 3);
    }

    #[test]
    fn decode_empty_body_returns_error() {
        assert!(decode_catchup_response(&[]).is_err());
    }

    #[test]
    fn encode_empty_mutations_list() {
        let encoded = encode_catchup_response(&[]).unwrap();
        let decoded = decode_catchup_response(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn digest_response_roundtrip() {
        let mut ids = std::collections::HashSet::new();
        ids.insert([1u8; 16]);
        ids.insert([2u8; 16]);
        let encoded = encode_digest_response(&ids);
        let decoded = decode_digest_response(&encoded).unwrap().unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn digest_decline_is_not_a_list() {
        // A peer that reports "too large", "unavailable", or nothing at all
        // must be read as "no digest", never as "holds nothing".
        assert!(decode_digest_response(&[DIGEST_STATUS_TOO_LARGE])
            .unwrap()
            .is_none());
        assert!(decode_digest_response(&[DIGEST_STATUS_UNAVAILABLE])
            .unwrap()
            .is_none());
        assert!(decode_digest_response(&[]).unwrap().is_none());
    }

    #[test]
    fn digest_length_mismatch_is_an_error() {
        let mut body = vec![DIGEST_STATUS_COMPLETE];
        body.extend_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(&[7u8; 16]); // claims two ids, carries one
        assert!(decode_digest_response(&body).is_err());
    }

    #[test]
    fn digest_limit_zero_means_default() {
        assert_eq!(decode_digest_limit(&[]), 0);
        assert_eq!(
            decode_digest_limit(&0u32.to_be_bytes()),
            DEFAULT_DIGEST_LIMIT
        );
        assert_eq!(decode_digest_limit(&9u32.to_be_bytes()), 9);
    }
}
