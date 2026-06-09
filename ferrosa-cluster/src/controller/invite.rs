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

#[cfg(test)]
mod tests {
    use super::*;

    /// Storm-prevention contract for the invite *connect* path
    /// (`recent_invite_connects`): a set of unreachable discovered peers
    /// re-offered on every invite round must be dialed at most once per
    /// cooldown window, not once per round. Without the cooldown gate the
    /// connect attempts scale with the number of rounds — the unbounded
    /// behavior that exhausted the local ephemeral port range and wedged all
    /// outbound networking.
    #[test]
    fn connect_cooldown_bounds_attempts_for_unreachable_peers() {
        let cooldown = Duration::from_secs(30);
        let capacity = 1000;
        let base = Instant::now();
        let peers: Vec<Uuid> = (0..50).map(|_| Uuid::new_v4()).collect();

        let mut recent: BTreeMap<Uuid, Instant> = BTreeMap::new();
        let mut attempts = 0usize;

        // 100 invite rounds, all within one cooldown window (1s apart).
        for round in 0..100u64 {
            let now = base + Duration::from_secs(round); // 0s..99s — but check happens per round
            for &peer in &peers {
                // Reservation timestamps advance, but each peer is only
                // re-allowed once `cooldown` has elapsed since its last attempt.
                if reserve_reconnect_invite(&mut recent, peer, now, cooldown, capacity) {
                    attempts += 1;
                }
            }
        }

        // Over 100 seconds with a 30s cooldown, each of the 50 peers is dialed
        // at rounds ~0, ~30, ~60, ~90 → 4 attempts each, NOT 100 each.
        assert_eq!(
            attempts,
            peers.len() * 4,
            "connect attempts must be cooldown-bounded, not once-per-round"
        );
        assert!(
            attempts < peers.len() * 100,
            "cooldown gate must collapse the per-round connection storm"
        );
    }

    /// A peer that just failed is suppressed within the window, then allowed
    /// again exactly once the cooldown elapses.
    #[test]
    fn connect_cooldown_allows_retry_after_window() {
        let cooldown = Duration::from_secs(30);
        let base = Instant::now();
        let peer = Uuid::new_v4();
        let mut recent: BTreeMap<Uuid, Instant> = BTreeMap::new();

        assert!(reserve_reconnect_invite(
            &mut recent,
            peer,
            base,
            cooldown,
            1000
        ));
        assert!(
            !reserve_reconnect_invite(
                &mut recent,
                peer,
                base + Duration::from_secs(5),
                cooldown,
                1000
            ),
            "within cooldown → suppressed"
        );
        assert!(
            reserve_reconnect_invite(
                &mut recent,
                peer,
                base + Duration::from_secs(31),
                cooldown,
                1000
            ),
            "after cooldown → one retry allowed"
        );
    }
}
