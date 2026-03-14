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
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key,
            rows: vec![row],
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
}
