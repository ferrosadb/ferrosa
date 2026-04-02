//! Token generation and schema sync helpers.

use bytes::Bytes;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_schema::Schema;
use uuid::Uuid;

/// Send the full schema snapshot to a peer over the bulk lane.
///
/// Used both after a force-promote rejoin (to sync schema + data replay) and
/// after a normal pair reconnection (to catch up schema changes the secondary
/// missed while it was offline).
pub(super) async fn send_schema_sync_to_peer(pm: &PeerManager, peer_host_id: Uuid, schema: &Schema) {
    let snap = schema.snapshot();
    let wire_snap = crate::pair::ddl::WireSchemaSnapshot::from_snapshot(&snap);
    match serde_json::to_vec(&wire_snap) {
        Ok(json) => {
            match pm
                .send(
                    peer_host_id,
                    Message::PairSchemaSync(Bytes::from(json)),
                    Lane::Bulk,
                )
                .await
            {
                Ok(_) => tracing::info!("schema snapshot sent to rejoined peer"),
                Err(e) => tracing::warn!(%e, "failed to send schema snapshot"),
            }
        }
        Err(e) => tracing::warn!(%e, "failed to serialize schema snapshot"),
    }
}

/// Generate a deterministic token for a node.
///
/// Uses a hash-like mixing of node_id and token index to produce well-distributed
/// token values across the i64 range. All nodes running the same code will compute
/// the same token assignments for the same (node_id, index) pair.
pub(crate) fn generate_deterministic_token(node_id: u64, index: usize) -> i64 {
    // Simple but effective: use wrapping multiply with a prime and XOR to spread bits.
    let mut h = node_id.wrapping_mul(0x517cc1b727220a95);
    h ^= (index as u64).wrapping_mul(0x6c62272e07bb0142);
    h = h.wrapping_mul(0x2545F4914F6CDD1D);
    h ^= h >> 32;
    h as i64
}
