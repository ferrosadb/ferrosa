//! Pair handoff and catch-up admission.
//! Correctness: withdraw old authority before granting the peer authority.
//! Last revised: 2026-10-29
//! Last changed: Keep the initiator demoted after lost or cancelled transfer replies.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Timeout for switchover RPC.
const SWITCHOVER_TIMEOUT: Duration = Duration::from_secs(10);

/// Progress of the data catch-up a promoted primary replays to a rejoined
/// peer. A switchover must not hand the primary role to a peer whose replay
/// has not finished: it would serve reads missing the replayed rows.
#[derive(Debug, Default)]
pub struct CatchUpGate(AtomicU8);

const CATCH_UP_NOT_PENDING: u8 = 0;
const CATCH_UP_IN_PROGRESS: u8 = 1;
const CATCH_UP_FAILED: u8 = 2;

impl CatchUpGate {
    /// A replay to the peer has started.
    pub fn begin(&self) {
        self.0.store(CATCH_UP_IN_PROGRESS, Ordering::SeqCst);
    }

    /// The replay finished and every mutation was acknowledged.
    pub fn complete(&self) {
        self.0.store(CATCH_UP_NOT_PENDING, Ordering::SeqCst);
    }

    /// The replay stopped short; the peer is missing data until a new
    /// catch-up runs.
    pub fn fail(&self) {
        self.0.store(CATCH_UP_FAILED, Ordering::SeqCst);
    }

    /// `Ok` when no catch-up is outstanding.
    pub fn ready(&self) -> Result<()> {
        match self.0.load(Ordering::SeqCst) {
            CATCH_UP_NOT_PENDING => Ok(()),
            CATCH_UP_IN_PROGRESS => Err(ClusterError::ModeTransitionRejected(
                "switchover refused: the data catch-up replay to the peer is still running; \
                 retry when the log shows `catch-up replay complete`"
                    .into(),
            )),
            _ => Err(ClusterError::ModeTransitionRejected(
                "switchover refused: the data catch-up replay to the peer FAILED, so the peer \
                 is missing writes; reconnect the peer to rerun catch-up (see the warn line \
                 naming the failed step)"
                    .into(),
            )),
        }
    }
}

/// Push this node's schema to the peer and require the peer to acknowledge
/// with the schema version it converged to.
///
/// `PairSchemaSyncHandler` acks with the version only after the peer's
/// keyspaces and tables match the snapshot. Anything else -- an error, a
/// timeout, an empty ack from an older peer, a different version -- leaves
/// the peer's schema unconfirmed and is returned as an error.
pub async fn push_schema_and_confirm(
    peer_manager: &PeerManager,
    peer_host_id: Uuid,
    schema: &ferrosa_schema::Schema,
) -> Result<()> {
    let snapshot = schema.snapshot();
    let wire = crate::pair::ddl::WireSchemaSnapshot::from_snapshot(&snapshot);
    let body = serde_json::to_vec(&wire)
        .map_err(|e| ClusterError::Internal(format!("serialize schema snapshot: {e}")))?;
    let resp = peer_manager
        .send_with_timeout(
            peer_host_id,
            Message::PairSchemaSync(bytes::Bytes::from(body)),
            Lane::Bulk,
            SWITCHOVER_TIMEOUT,
        )
        .await
        .map_err(|e| {
            ClusterError::ModeTransitionRejected(format!(
                "peer schema unconfirmed: pushing the schema to {peer_host_id} failed ({e})"
            ))
        })?;
    match resp {
        Message::PairDdlAck(version) if version.as_ref() == snapshot.version.as_bytes() => Ok(()),
        Message::PairDdlAck(version) => Err(ClusterError::ModeTransitionRejected(format!(
            "peer schema unconfirmed: {peer_host_id} acknowledged the schema push without \
             reporting convergence to version {} (ack carried {} bytes)",
            snapshot.version,
            version.len()
        ))),
        other => Err(ClusterError::ModeTransitionRejected(format!(
            "peer schema unconfirmed: expected PairDdlAck from {peer_host_id}, got {:?}",
            other.msg_type()
        ))),
    }
}

/// Initiate switchover from the primary side.
///
/// Withdraw local authority before sending a versioned grant to the secondary.
/// A timeout or cancellation cannot prove the peer did not accept the grant,
/// so it must never restore the old primary role.
pub async fn initiate_switchover(
    peer_manager: &PeerManager,
    local_host_id: Uuid,
    peer_host_id: Uuid,
    role: &arc_swap::ArcSwap<PairRole>,
    root: &std::path::Path,
) -> Result<()> {
    if **role.load() != PairRole::Primary {
        return Err(ClusterError::NotPrimary);
    }
    if peer_manager
        .peer_capabilities(peer_host_id)
        .await
        .unwrap_or(0)
        & ferrosa_net::handshake::CAP_PAIR_AUTHORITY
        == 0
    {
        return Err(ClusterError::ReplicationFailed(
            "peer lacks versioned pair authority capability; upgrade both peers".into(),
        ));
    }
    let response = peer_manager
        .send_with_timeout(
            peer_host_id,
            wire(&AuthorityRpc::Prepare),
            Lane::Raft,
            SWITCHOVER_TIMEOUT,
        )
        .await?;
    let (challenge, peer_generation) = match decode(response) {
        Some(AuthorityRpc::Ready {
            challenge,
            generation,
        }) => (challenge, generation),
        _ => {
            return Err(ClusterError::ReplicationFailed(
                "invalid pair authority preparation".into(),
            ))
        }
    };
    let current = super::authority::PairAuthority::read(root, local_host_id)?;
    let generation = current
        .map_or(0, |r| r.generation)
        .max(peer_generation)
        .checked_add(1)
        .ok_or_else(|| ClusterError::Internal("pair authority generation exhausted".into()))?;
    // No await between withdrawal and persistence. Failure leaves this process fenced.
    role.store(Arc::new(PairRole::Secondary));
    super::authority::PairAuthority::store_generation(
        root,
        local_host_id,
        peer_host_id,
        peer_host_id,
        generation,
    )?;
    let response = peer_manager
        .send_with_timeout(
            peer_host_id,
            wire(&AuthorityRpc::Grant {
                challenge,
                generation,
            }),
            Lane::Raft,
            SWITCHOVER_TIMEOUT,
        )
        .await?;
    match decode(response) {
        Some(AuthorityRpc::Accepted {
            generation: accepted,
        }) if accepted == generation => Ok(()),
        _ => Err(ClusterError::ReplicationFailed(
            "pair authority grant unconfirmed; local writer remains fenced".into(),
        )),
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
enum AuthorityRpc {
    Prepare,
    Ready { challenge: Uuid, generation: u64 },
    Grant { challenge: Uuid, generation: u64 },
    Accepted { generation: u64 },
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityEnvelope {
    version: u32,
    payload: AuthorityRpc,
}
fn wire(message: &AuthorityRpc) -> Message {
    Message::PairAuthority(
        serde_json::to_vec(&AuthorityEnvelope {
            version: 1,
            payload: message.clone(),
        })
        .expect("serializable authority RPC")
        .into(),
    )
}
fn decode(message: Message) -> Option<AuthorityRpc> {
    match message {
        Message::PairAuthority(bytes) if bytes.len() <= 4096 => {
            let envelope: AuthorityEnvelope = serde_json::from_slice(&bytes).ok()?;
            (envelope.version == 1).then_some(envelope.payload)
        }
        _ => None,
    }
}

/// A versioned, peer-bound grant consumes one fresh receiver challenge.
/// Legacy unversioned assignments are always refused.
pub struct RoleSwapHandler {
    local_host_id: Uuid,
    role: Arc<arc_swap::ArcSwap<PairRole>>,
    durable: Option<(Uuid, std::path::PathBuf)>,
    challenge: parking_lot::Mutex<Option<Uuid>>,
    gate: Arc<tokio::sync::RwLock<()>>,
}
impl RoleSwapHandler {
    pub fn new(local_host_id: Uuid, role: Arc<arc_swap::ArcSwap<PairRole>>) -> Self {
        Self {
            local_host_id,
            role,
            durable: None,
            challenge: parking_lot::Mutex::new(None),
            gate: Arc::new(tokio::sync::RwLock::new(())),
        }
    }
    pub fn durable(
        local_host_id: Uuid,
        peer: Uuid,
        role: Arc<arc_swap::ArcSwap<PairRole>>,
        root: std::path::PathBuf,
        gate: Arc<tokio::sync::RwLock<()>>,
    ) -> Self {
        Self {
            durable: Some((peer, root)),
            gate,
            ..Self::new(local_host_id, role)
        }
    }
}
#[async_trait::async_trait]
impl RpcHandler for RoleSwapHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let (peer, root) = self.durable.as_ref()?;
        if from.0 != *peer {
            return None;
        }
        let request = decode(msg)?;
        let _gate = self.gate.write().await;
        let record = super::authority::PairAuthority::read(root, self.local_host_id).ok()?;
        if record.as_ref().is_some_and(|r| r.peer != *peer) {
            return None;
        }
        let current = record.map_or(0, |r| r.generation);
        match request {
            AuthorityRpc::Prepare => {
                if **self.role.load() != PairRole::Secondary {
                    return None;
                }
                let challenge = Uuid::new_v4();
                *self.challenge.lock() = Some(challenge);
                Some(wire(&AuthorityRpc::Ready {
                    challenge,
                    generation: current,
                }))
            }
            AuthorityRpc::Grant {
                challenge,
                generation,
            } => {
                if generation <= current
                    || self.challenge.lock().take() != Some(challenge)
                    || **self.role.load() != PairRole::Secondary
                {
                    return None;
                }
                super::authority::PairAuthority::store_generation(
                    root,
                    self.local_host_id,
                    *peer,
                    self.local_host_id,
                    generation,
                )
                .ok()?;
                self.role.store(Arc::new(PairRole::Primary));
                Some(wire(&AuthorityRpc::Accepted { generation }))
            }
            _ => None,
        }
    }
}
