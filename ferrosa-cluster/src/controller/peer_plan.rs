//! Pure peer-event planning seams.
//!
//! This module contains no side effects. It translates a snapshot of controller
//! state into actions that `peer_events` can execute while holding the required
//! locks. Keeping policy here makes recovery behavior testable without live
//! networking, Raft, or hint-delivery tasks.

use std::net::SocketAddr;
use std::time::Instant;

use uuid::Uuid;

use crate::mode::DeploymentMode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PeerEventAction {
    TrackPeer {
        host_id: Uuid,
        addr: SocketAddr,
    },
    TransitionToPair {
        host_id: Uuid,
        addr: SocketAddr,
        inbound: bool,
    },
    TransitionToForming,
    TriggerClusterJoin {
        host_id: Uuid,
        addr: SocketAddr,
        cql_broadcast: Option<String>,
        internode_broadcast: Option<String>,
    },
    /// Reached committed quorum while this node has no in-process Raft.
    ///
    /// `RestoreClusterMode` only flips the mode; it assumes Raft is already
    /// running, which is true when DegradedCluster was reached by losing
    /// quorum inside a live process. It is false for a node that *started*
    /// in DegradedCluster from its `cluster-member` marker: that process has
    /// never initialised Raft, so flipping the mode leaves it in Cluster with
    /// no Raft, refusing every query forever.
    RejoinClusterWithRaftInit,
    SendClusterInvite {
        host_id: Uuid,
        force: bool,
    },
    RestoreClusterMode,
    DeliverHints {
        host_id: Uuid,
    },
}

#[derive(Debug, Clone)]
pub(super) struct PeerConnectPlanInput {
    pub(super) mode: DeploymentMode,
    pub(super) host_id: Uuid,
    pub(super) addr: SocketAddr,
    pub(super) inbound: bool,
    pub(super) connected_peers_after_track: Vec<(Uuid, SocketAddr)>,
    pub(super) committed_cluster_size: usize,
    pub(super) join_enqueued: bool,
    pub(super) last_invite_sent: Option<Instant>,
    pub(super) now: Instant,
    pub(super) cql_broadcast: Option<String>,
    pub(super) internode_broadcast: Option<String>,
    /// Whether this process already has a Raft instance installed.
    pub(super) raft_initialized: bool,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct ConnectedPeerInput {
    pub(super) host_id: Uuid,
    pub(super) addr: SocketAddr,
    pub(super) mode: DeploymentMode,
    pub(super) known_peer_count_after_tracking: usize,
    pub(super) committed_cluster_size: usize,
    pub(super) join_enqueued: bool,
    pub(super) last_reconnect_invite_sent: Option<Instant>,
    pub(super) cql_broadcast: Option<String>,
    pub(super) internode_broadcast: Option<String>,
    pub(super) now: Instant,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct ConnectedPeerPlan {
    pub(super) actions: Vec<PeerEventAction>,
}

#[derive(Debug, Clone)]
pub(super) struct PeerRecoveredPlanInput {
    pub(super) mode: DeploymentMode,
    pub(super) host_id: Uuid,
    pub(super) pending_hint_count: usize,
    pub(super) peer_manager_available: bool,
}
#[cfg(test)]
pub(super) type RecoveredPeerInput = PeerRecoveredPlanInput;

#[cfg(test)]
pub(super) fn plan_connected_peer(input: ConnectedPeerInput) -> ConnectedPeerPlan {
    ConnectedPeerPlan {
        actions: plan_peer_connected(PeerConnectPlanInput {
            mode: input.mode,
            host_id: input.host_id,
            addr: input.addr,
            inbound: false,
            connected_peers_after_track: vec![
                (input.host_id, input.addr);
                input.known_peer_count_after_tracking
            ],
            committed_cluster_size: input.committed_cluster_size,
            join_enqueued: input.join_enqueued,
            last_invite_sent: input.last_reconnect_invite_sent,
            now: input.now,
            cql_broadcast: input.cql_broadcast,
            internode_broadcast: input.internode_broadcast,
            raft_initialized: true,
        })
        .into_iter()
        .filter(|action| !matches!(action, PeerEventAction::TrackPeer { .. }))
        .collect(),
    }
}

pub(super) fn plan_peer_connected(input: PeerConnectPlanInput) -> Vec<PeerEventAction> {
    let mut actions = vec![PeerEventAction::TrackPeer {
        host_id: input.host_id,
        addr: input.addr,
    }];

    match input.mode {
        DeploymentMode::Standalone => actions.push(PeerEventAction::TransitionToPair {
            host_id: input.host_id,
            addr: input.addr,
            inbound: input.inbound,
        }),
        DeploymentMode::Pair => {
            if input.connected_peers_after_track.len() >= 2 {
                actions.push(PeerEventAction::TransitionToForming);
            }
        }
        DeploymentMode::Cluster => {
            actions.push(PeerEventAction::TriggerClusterJoin {
                host_id: input.host_id,
                addr: input.addr,
                cql_broadcast: input.cql_broadcast,
                internode_broadcast: input.internode_broadcast,
            });
            if should_send_cluster_invite(input.join_enqueued, input.last_invite_sent, input.now) {
                actions.push(PeerEventAction::SendClusterInvite {
                    host_id: input.host_id,
                    force: false,
                });
            }
        }
        DeploymentMode::Forming | DeploymentMode::DegradedPair => {}
        DeploymentMode::DegradedCluster => {
            if committed_quorum_reached(
                input.connected_peers_after_track.len(),
                input.committed_cluster_size,
            ) {
                if input.raft_initialized {
                    actions.push(PeerEventAction::RestoreClusterMode);
                } else {
                    actions.push(PeerEventAction::RejoinClusterWithRaftInit);
                }
            }
        }
    }

    actions
}

#[cfg(test)]
pub(super) fn plan_recovered_peer(input: RecoveredPeerInput) -> ConnectedPeerPlan {
    ConnectedPeerPlan {
        actions: plan_peer_recovered(input),
    }
}

pub(super) fn plan_peer_recovered(input: PeerRecoveredPlanInput) -> Vec<PeerEventAction> {
    let mut actions = Vec::new();

    if matches!(
        input.mode,
        DeploymentMode::Cluster | DeploymentMode::DegradedCluster
    ) {
        actions.push(PeerEventAction::SendClusterInvite {
            host_id: input.host_id,
            force: false,
        });
    }

    if input.pending_hint_count > 0 && input.peer_manager_available {
        actions.push(PeerEventAction::DeliverHints {
            host_id: input.host_id,
        });
    }

    actions
}

pub(super) fn should_send_cluster_invite(
    join_enqueued: bool,
    last_sent: Option<Instant>,
    now: Instant,
) -> bool {
    join_enqueued
        || last_sent
            .map(|sent| now.duration_since(sent) >= super::CLUSTER_RECONNECT_INVITE_COOLDOWN)
            .unwrap_or(true)
}

fn committed_quorum_reached(connected_peer_count: usize, committed_cluster_size: usize) -> bool {
    let total = if committed_cluster_size > 0 {
        committed_cluster_size
    } else {
        connected_peer_count + 1
    };
    let quorum = (total / 2) + 1;
    connected_peer_count + 1 >= quorum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn addr(port: u16) -> SocketAddr {
        ([127, 0, 0, 1], port).into()
    }

    #[test]
    fn cluster_connect_existing_member_plans_join_and_invite_when_not_recently_invited() {
        let peer = host(2);
        let now = Instant::now();

        let actions = plan_peer_connected(PeerConnectPlanInput {
            mode: DeploymentMode::Cluster,
            host_id: peer,
            addr: addr(7001),
            inbound: false,
            connected_peers_after_track: vec![(peer, addr(7001))],
            committed_cluster_size: 3,
            join_enqueued: false,
            last_invite_sent: None,
            now,
            cql_broadcast: Some("127.0.0.1:9043".to_string()),
            internode_broadcast: Some("peer-node:7001".to_string()),
            raft_initialized: true,
        });

        assert_eq!(
            actions,
            vec![
                PeerEventAction::TrackPeer {
                    host_id: peer,
                    addr: addr(7001),
                },
                PeerEventAction::TriggerClusterJoin {
                    host_id: peer,
                    addr: addr(7001),
                    cql_broadcast: Some("127.0.0.1:9043".to_string()),
                    internode_broadcast: Some("peer-node:7001".to_string()),
                },
                PeerEventAction::SendClusterInvite {
                    host_id: peer,
                    force: false,
                },
            ]
        );
    }

    #[test]
    fn cluster_connect_existing_member_suppresses_duplicate_invite_with_recent_reservation() {
        let peer = host(2);
        let now = Instant::now();

        let actions = plan_peer_connected(PeerConnectPlanInput {
            mode: DeploymentMode::Cluster,
            host_id: peer,
            addr: addr(7001),
            inbound: false,
            connected_peers_after_track: vec![(peer, addr(7001))],
            committed_cluster_size: 3,
            join_enqueued: false,
            last_invite_sent: Some(now),
            now,
            cql_broadcast: None,
            internode_broadcast: None,
            raft_initialized: true,
        });

        assert!(actions.contains(&PeerEventAction::TriggerClusterJoin {
            host_id: peer,
            addr: addr(7001),
            cql_broadcast: None,
            internode_broadcast: None,
        }));
        assert!(
            !actions.iter().any(|action| matches!(
                action,
                PeerEventAction::SendClusterInvite { host_id, .. } if *host_id == peer
            )),
            "recent invite reservation should suppress only duplicate invite delivery, not join planning"
        );
    }

    /// A node that *starts* in DegradedCluster from its `cluster-member`
    /// marker has never initialised Raft in this process. Reaching quorum
    /// must therefore rebuild Raft, not merely flip the mode.
    ///
    /// Observed live on 2026-08-20: node3 restarted, read its marker, came up
    /// DegradedCluster, reached quorum, flipped to Cluster -- and then sat at
    /// `503 {"detail":"raft not yet initialized"}` for thirteen minutes,
    /// because `RestoreClusterMode` assumes a Raft that a restart destroyed.
    #[test]
    fn returning_member_without_raft_rejoins_by_initialising_it() {
        let peer = host(2);
        let plan = plan_peer_connected(PeerConnectPlanInput {
            mode: DeploymentMode::DegradedCluster,
            host_id: peer,
            addr: addr(7001),
            inbound: false,
            connected_peers_after_track: vec![(peer, addr(7001)), (host(3), addr(7002))],
            committed_cluster_size: 3,
            join_enqueued: false,
            last_invite_sent: None,
            now: Instant::now(),
            cql_broadcast: None,
            internode_broadcast: None,
            raft_initialized: false,
        });

        assert!(
            plan.contains(&PeerEventAction::RejoinClusterWithRaftInit),
            "a returning member with no Raft must be told to initialise it, \
             not just to change its mode label: {plan:?}"
        );
        assert!(
            !plan.contains(&PeerEventAction::RestoreClusterMode),
            "RestoreClusterMode only flips the mode and would leave this node \
             in Cluster with no Raft, refusing every query: {plan:?}"
        );
    }

    /// The in-process case is unchanged: quorum was lost and regained without
    /// a restart, so Raft is still installed and only the mode needs restoring.
    #[test]
    fn quorum_regained_in_process_only_restores_the_mode() {
        let peer = host(2);
        let plan = plan_peer_connected(PeerConnectPlanInput {
            mode: DeploymentMode::DegradedCluster,
            host_id: peer,
            addr: addr(7001),
            inbound: false,
            connected_peers_after_track: vec![(peer, addr(7001)), (host(3), addr(7002))],
            committed_cluster_size: 3,
            join_enqueued: false,
            last_invite_sent: None,
            now: Instant::now(),
            cql_broadcast: None,
            internode_broadcast: None,
            raft_initialized: true,
        });

        assert!(
            plan.contains(&PeerEventAction::RestoreClusterMode),
            "{plan:?}"
        );
        assert!(
            !plan.contains(&PeerEventAction::RejoinClusterWithRaftInit),
            "re-initialising a live Raft would discard in-flight state: {plan:?}"
        );
    }

    #[test]
    fn degraded_cluster_peer_connect_plans_mode_restore_only_when_committed_quorum_reached() {
        let peer = host(2);
        let now = Instant::now();
        let one_peer = vec![(peer, addr(7001))];
        let two_peers = vec![(peer, addr(7001)), (host(3), addr(7002))];

        let no_quorum = plan_peer_connected(PeerConnectPlanInput {
            mode: DeploymentMode::DegradedCluster,
            host_id: peer,
            addr: addr(7001),
            inbound: false,
            connected_peers_after_track: one_peer,
            committed_cluster_size: 5,
            join_enqueued: false,
            last_invite_sent: None,
            now,
            cql_broadcast: None,
            internode_broadcast: None,
            raft_initialized: true,
        });
        assert!(!no_quorum.contains(&PeerEventAction::RestoreClusterMode));

        let quorum = plan_peer_connected(PeerConnectPlanInput {
            mode: DeploymentMode::DegradedCluster,
            host_id: peer,
            addr: addr(7001),
            inbound: false,
            connected_peers_after_track: two_peers,
            committed_cluster_size: 5,
            join_enqueued: false,
            last_invite_sent: None,
            now,
            cql_broadcast: None,
            internode_broadcast: None,
            raft_initialized: true,
        });
        assert!(quorum.contains(&PeerEventAction::RestoreClusterMode));
    }

    #[test]
    fn peer_recovered_in_cluster_plans_invite_and_hint_delivery_independently() {
        let peer = host(2);

        assert_eq!(
            plan_peer_recovered(PeerRecoveredPlanInput {
                mode: DeploymentMode::Cluster,
                host_id: peer,
                pending_hint_count: 0,
                peer_manager_available: false,
            }),
            vec![PeerEventAction::SendClusterInvite {
                host_id: peer,
                force: false,
            }],
            "cluster invite recovery must not depend on pending hints or a hint-delivery PeerManager"
        );

        assert_eq!(
            plan_peer_recovered(PeerRecoveredPlanInput {
                mode: DeploymentMode::Cluster,
                host_id: peer,
                pending_hint_count: 7,
                peer_manager_available: true,
            }),
            vec![
                PeerEventAction::SendClusterInvite {
                    host_id: peer,
                    force: false,
                },
                PeerEventAction::DeliverHints { host_id: peer },
            ]
        );
    }
}
