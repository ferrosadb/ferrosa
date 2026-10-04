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
/// Sends `RoleSwap` to the secondary, then swaps local role.
pub async fn initiate_switchover(
    peer_manager: &PeerManager,
    local_host_id: Uuid,
    peer_host_id: Uuid,
    role: &arc_swap::ArcSwap<PairRole>,
) -> Result<()> {
    if **role.load() != PairRole::Primary {
        return Err(ClusterError::NotPrimary);
    }

    let resp = peer_manager
        .send_with_timeout(
            peer_host_id,
            Message::RoleSwap {
                new_primary: peer_host_id,
                new_secondary: local_host_id,
            },
            Lane::Raft,
            SWITCHOVER_TIMEOUT,
        )
        .await
        .map_err(ClusterError::Net)?;

    match resp {
        Message::RoleSwap {
            new_primary,
            new_secondary,
        } => {
            if new_primary != peer_host_id || new_secondary != local_host_id {
                return Err(ClusterError::ReplicationFailed(
                    "role swap response mismatch".into(),
                ));
            }
        }
        other => {
            return Err(ClusterError::ReplicationFailed(format!(
                "expected RoleSwap response, got {:?}",
                other.msg_type()
            )));
        }
    }

    role.store(Arc::new(PairRole::Secondary));
    tracing::info!("switchover complete: demoted to secondary");
    Ok(())
}

/// RPC handler for RoleSwap messages (runs on secondary).
pub struct RoleSwapHandler {
    local_host_id: Uuid,
    role: Arc<arc_swap::ArcSwap<PairRole>>,
}

impl RoleSwapHandler {
    pub fn new(local_host_id: Uuid, role: Arc<arc_swap::ArcSwap<PairRole>>) -> Self {
        Self {
            local_host_id,
            role,
        }
    }
}

#[async_trait::async_trait]
impl RpcHandler for RoleSwapHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let (new_primary, new_secondary) = match msg {
            Message::RoleSwap {
                new_primary,
                new_secondary,
            } => (new_primary, new_secondary),
            _ => return None,
        };

        // Determine our new role based on the assignment.
        let new_role = if new_primary == self.local_host_id {
            PairRole::Primary
        } else if new_secondary == self.local_host_id {
            PairRole::Secondary
        } else {
            tracing::error!(
                "role swap: neither primary={} nor secondary={} matches local={}",
                new_primary,
                new_secondary,
                self.local_host_id,
            );
            return None;
        };

        self.role.store(Arc::new(new_role));
        tracing::info!(%new_role, "role swap complete");

        Some(Message::RoleSwap {
            new_primary,
            new_secondary,
        })
    }
}
