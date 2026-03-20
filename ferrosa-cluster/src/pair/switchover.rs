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

    let resp = tokio::time::timeout(
        SWITCHOVER_TIMEOUT,
        peer_manager.send(
            peer_host_id,
            Message::RoleSwap {
                new_primary: peer_host_id,
                new_secondary: local_host_id,
            },
            Lane::Raft,
        ),
    )
    .await
    .map_err(|_| {
        ClusterError::Net(ferrosa_net::error::NetError::Timeout(
            "switchover timed out waiting for peer".into(),
        ))
    })?
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
