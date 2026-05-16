//! Pure planning helpers for reconnect `ClusterInvite` delivery.
//!
//! Reconnect callbacks can arrive in bursts from outbound, inbound, and
//! recovered-peer paths. The reservation helper keeps the cooldown check and
//! timestamp update under one caller-held lock so only one callback schedules a
//! delivery for a peer in a cooldown window.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub(super) struct ReconnectInvitePlanInput<'a> {
    pub(super) local_host_id: Uuid,
    pub(super) local_addr: Option<SocketAddr>,
    pub(super) recipient: Uuid,
    pub(super) connected_peers: &'a [(Uuid, SocketAddr)],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReconnectInvitePlan {
    pub(super) recipient: Uuid,
    pub(super) initiator: Uuid,
    pub(super) peers: Vec<(Uuid, SocketAddr)>,
}

pub(super) fn plan_reconnect_invite(
    input: ReconnectInvitePlanInput<'_>,
) -> Option<ReconnectInvitePlan> {
    let mut peers: Vec<(Uuid, SocketAddr)> = input
        .connected_peers
        .iter()
        .filter(|(id, _)| *id != input.recipient && *id != input.local_host_id)
        .copied()
        .collect();

    if let Some(local_addr) = input.local_addr {
        if !peers.iter().any(|(id, _)| *id == input.local_host_id) {
            peers.push((input.local_host_id, local_addr));
        }
    }

    if peers.is_empty() {
        return None;
    }

    Some(ReconnectInvitePlan {
        recipient: input.recipient,
        initiator: input.local_host_id,
        peers,
    })
}

pub(super) fn reserve_reconnect_invite(
    recent: &mut BTreeMap<Uuid, Instant>,
    recipient: Uuid,
    now: Instant,
    cooldown: Duration,
    capacity: usize,
) -> bool {
    if recent
        .get(&recipient)
        .map(|sent| now.duration_since(*sent) < cooldown)
        .unwrap_or(false)
    {
        return false;
    }

    if recent.len() >= capacity {
        recent.clear();
    }
    recent.insert(recipient, now);
    true
}
