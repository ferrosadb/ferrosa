//! A data center's relay to the *other* data centers.
//!
//! Inside a DC, nodes are locally connected and fully replicated: one Raft group
//! per DC, and a client's quorum is that DC's quorum. A DC→DC link crosses a
//! WAN, so it carries **one copy per remote DC per operation**, never one copy
//! per remote node, and it is reached through whatever live node that DC
//! announces.
//!
//! This module is the seam between the two: it builds the DC-level registry from
//! what this node has actually learned at runtime (the peers it is connected to,
//! and the DC each announced in its handshake), and hands a committed DDL to
//! every remote DC through that DC's relay.
//!
//! The receiving side needs no new protocol surface: a relayed DDL goes over the
//! same [`Message::PairDdlForward`] the intra-DC leader-forward path uses, and
//! the receiving node's `ClusterDdlForwardHandler` hands it to *its* leader, so
//! the copy lands in that DC's own Raft group — and that group's mesh takes it to
//! the DC's quorum.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use uuid::Uuid;

use crate::accord::cross_dc_dispatch::{
    dc_registry, outcome_for_report, CrossDcDispatch, CrossDcOutcome, CrossDcSender, DcContact,
    DcLink, FanoutReport,
};
use crate::consistency::ConsistencyLevel;
use crate::pair::ddl::DdlOperation;

use super::ModeController;

impl ModeController {
    /// This DC's view of every OTHER DC: one link per remote DC, each holding
    /// the nodes this one knows there.
    ///
    /// Built from what the node has learned at runtime — `connected_peers` and
    /// the DC each peer announced — never from configuration, so a DC that has
    /// not connected yet is simply not a relay target. A peer that announced no
    /// DC is not in it either: everywhere else such a peer is treated as being
    /// in *this* DC, and addressing it as a remote DC would send a cross-DC copy
    /// to a node that is probably right here.
    pub fn cross_dc_registry(&self) -> BTreeMap<String, DcLink> {
        // Same idiom as `connected_peers_for_test`: a `parking_lot`-style guard,
        // so no poisoning to handle.
        let peers: Arc<Vec<(Uuid, SocketAddr)>> = self.connected_peers.load_full();
        dc_registry(&peers, &self.peer_dcs_snapshot(), &self.config.data_center)
    }

    /// Relay one committed DDL to every remote DC, at the standard `cl` demands.
    ///
    /// Returns what actually happened rather than a boolean: `Complete` when
    /// every remote DC acknowledged, `PendingLocally` when the level does not
    /// wait on a remote DC and some are still owed the copy, `NotSatisfied` when
    /// the level *does* require every DC and did not get them, and `SingleDc`
    /// when there is no other DC to tell.
    pub async fn relay_ddl(&self, op: &DdlOperation, cl: ConsistencyLevel) -> CrossDcOutcome {
        let registry = self.cross_dc_registry();
        if registry.is_empty() {
            return CrossDcOutcome::SingleDc;
        }
        let Some(peer_manager) = self.peer_manager_arc() else {
            // No transport at all: every remote DC is owed the copy, and saying
            // so is the only honest answer — the caller (and the CL policy)
            // decides whether that is survivable.
            let failed: Vec<String> = registry.keys().cloned().collect();
            tracing::error!(
                owed = ?failed,
                "cross-DC DDL relay requested with no peer manager: no remote DC can be reached"
            );
            return outcome_for_report(
                FanoutReport {
                    failed,
                    ..FanoutReport::default()
                },
                cl,
                registry.len(),
            );
        };
        let sender = DdlRelaySender { peer_manager };
        CrossDcDispatch::default()
            .deliver_for_cl(cl, &sender, op, &registry)
            .await
    }
}

#[async_trait::async_trait]
impl crate::ddl_path::DdlRelay for ModeController {
    async fn relay_committed_ddl(&self, op: &DdlOperation, cl: ConsistencyLevel) -> CrossDcOutcome {
        self.relay_ddl(op, cl).await
    }
}

/// Hands a relayed DDL to a node in the remote DC over the ordinary internode
/// message the intra-DC leader forward uses.
///
/// The acknowledgement is that node's [`Message::PairDdlAck`] — which it only
/// sends after the DDL is committed *in its own DC's group*, so an ack means the
/// receiving DC's Raft has it, not that a socket accepted bytes.
pub struct DdlRelaySender {
    peer_manager: Arc<PeerManager>,
}

#[async_trait::async_trait]
impl CrossDcSender<DdlOperation> for DdlRelaySender {
    async fn send_to_contact(&self, contact: &DcContact, op: &DdlOperation) -> Result<(), String> {
        let body = op.to_bytes().map_err(|e| e.to_string())?;
        let sent = self
            .peer_manager
            .send(
                contact.host_id,
                Message::PairDdlForward(body.clone()),
                Lane::Data,
            )
            .await;

        let response = match sent {
            Ok(response) => response,
            // The contact is known from the registry but has no usable pool
            // (first contact, or an address that moved): dial it, then retry
            // once — the same recovery the intra-DC forward path uses.
            Err(e)
                if e.to_string().contains("unknown peer")
                    || e.to_string().contains("no connection pool") =>
            {
                let addr = self
                    .peer_manager
                    .peer_addr(contact.host_id)
                    .await
                    .ok_or_else(|| {
                        format!("no address for the relay contact {}", contact.host_id)
                    })?;
                self.peer_manager
                    .ensure_peer(contact.host_id, &addr)
                    .await
                    .map_err(|e| e.to_string())?;
                self.peer_manager
                    .send(contact.host_id, Message::PairDdlForward(body), Lane::Data)
                    .await
                    .map_err(|e| e.to_string())?
            }
            Err(e) => return Err(e.to_string()),
        };

        match response {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(format!(
                "unexpected reply to a relayed DDL from {}: {other:?}",
                contact.host_id
            )),
        }
    }
}
