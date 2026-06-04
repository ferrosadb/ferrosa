//! Automatic anti-entropy repair scheduler.
//!
//! Design: `specs/proposed/automatic-repair-scheduler-design.md`
//! FMEA:   `specs/proposed/automatic-repair-scheduler-fmea.md`
//!
//! The cluster already has the repair *primitives* (`RepairCoordinator::
//! repair_table` → bounded Merkle-diff-then-stream sessions). What was missing
//! for *automatic* repair is a deterministic driver that decides **which ranges
//! this node should initiate** so that, when every node runs the scheduler, each
//! token range is repaired by exactly **one** initiator — not once per replica.
//!
//! That decision is [`select_initiated_ranges`], a **pure function** of the ring
//! (no clock, no RNG, no IO — FMEA #4): for each range the local node replicates,
//! the initiator is the live replica with the **lowest `host_id`**. Only when the
//! local node *is* that initiator does the range appear in the result. This kills
//! the thundering-herd failure mode (FMEA #1) without an election: same ring →
//! same single initiator, recomputed each tick so membership churn self-corrects.

use crate::ring::TokenRing;

use super::{coordinator::owned_token_ranges, repair_participants};

/// One token range this node should initiate repair for, with the live peers to
/// repair against. `[start, end)` over the vnode partitioning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiatedRange {
    pub start: i64,
    pub end: i64,
    /// Live replica peers (excluding self) to run sessions against.
    pub peers: Vec<u64>,
}

/// Ranges the local node should **initiate** anti-entropy repair for this cycle,
/// as the deterministic single initiator.
///
/// For every range the local node replicates (at `rf`), the initiator is the
/// **live** replica with the lowest `host_id`. The range is returned only when
/// the local node is that initiator and at least one live peer exists. Pure
/// function of the ring — no IO/clock/RNG, so it is unit-testable and produces
/// the same selection on every node given the same ring (FMEA #1 herd, #4
/// determinism, #5 churn-idempotence).
pub fn select_initiated_ranges(
    ring: &TokenRing,
    local_node_id: u64,
    rf: usize,
) -> Vec<InitiatedRange> {
    // The local node must be in the ring to compare host_ids; if it isn't yet
    // (still joining), it initiates nothing.
    let local_host = match ring.get_node(local_node_id) {
        Some(info) => info.host_id,
        None => return Vec::new(),
    };

    let mut out = Vec::new();
    for (start, end) in owned_token_ranges(ring, local_node_id, rf) {
        // Live replicas of this range (down/joining nodes filtered out).
        let participants = repair_participants(ring, start, rf);

        // Peers to repair against = live participants other than self. With no
        // peer there is nothing to reconcile (single-node / all-peers-down).
        let peers: Vec<u64> = participants
            .iter()
            .copied()
            .filter(|&p| p != local_node_id)
            .collect();
        if peers.is_empty() {
            continue;
        }

        // Deterministic initiator = live participant with the lowest host_id.
        // host_id is the durable per-node identity (stable across restarts), so
        // the choice is stable for a given ring and survives node_id reuse.
        let initiator_host = participants
            .iter()
            .filter_map(|&n| ring.get_node(n).map(|info| info.host_id))
            .min();

        if initiator_host == Some(local_host) {
            out.push(InitiatedRange { start, end, peers });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeInfo, NodeState};
    use uuid::Uuid;

    fn node(addr: &str, host_id: u128, state: NodeState) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::from_u128(host_id),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state,
            cql_broadcast: None,
        }
    }

    /// 3-node ring, RF=3, with FIXED host_ids (1 < 2 < 3 by UUID order) so the
    /// initiator is deterministic and assertable. node_id N has host_id N.
    fn three_node_ring() -> TokenRing {
        let mut ring = TokenRing::new();
        ring.add_node(1, node("10.0.0.1:7000", 1, NodeState::Normal));
        ring.add_node(2, node("10.0.0.2:7000", 2, NodeState::Normal));
        ring.add_node(3, node("10.0.0.3:7000", 3, NodeState::Normal));
        ring.assign_tokens(1, &[100, 400, 700]);
        ring.assign_tokens(2, &[200, 500, 800]);
        ring.assign_tokens(3, &[300, 600, 900]);
        ring
    }

    /// FMEA #1 (thundering herd): with RF=3 on a 3-node ring every range's
    /// replica set is {1,2,3}, so the lowest-host_id node (node 1) is the sole
    /// initiator. node 1 selects every owned range; nodes 2 and 3 select none.
    #[test]
    fn exactly_one_initiator_per_range_no_herd() {
        let ring = three_node_ring();

        let n1 = select_initiated_ranges(&ring, 1, 3);
        let n2 = select_initiated_ranges(&ring, 2, 3);
        let n3 = select_initiated_ranges(&ring, 3, 3);

        assert!(!n1.is_empty(), "lowest-host_id node must initiate its ranges");
        assert!(n2.is_empty(), "node 2 must NOT initiate (node 1 is lower)");
        assert!(n3.is_empty(), "node 3 must NOT initiate (node 1 is lower)");

        // Every initiated range repairs against the other two replicas.
        for r in &n1 {
            let mut peers = r.peers.clone();
            peers.sort_unstable();
            assert_eq!(peers, vec![2, 3], "RF=3 peers = the two non-self replicas");
            assert!(r.start < r.end, "range must be non-empty [start,end)");
        }
    }

    /// FMEA #4 (determinism): same ring → identical selection, every call.
    #[test]
    fn selection_is_deterministic() {
        let ring = three_node_ring();
        let a = select_initiated_ranges(&ring, 1, 3);
        let b = select_initiated_ranges(&ring, 1, 3);
        assert_eq!(a, b);
    }

    /// FMEA #5 (membership churn): if the lowest-host_id node goes down, the
    /// next-lowest LIVE replica becomes the initiator — no range is orphaned and
    /// no two nodes initiate the same range.
    #[test]
    fn initiator_fails_over_when_lowest_host_down() {
        let mut ring = three_node_ring();
        // Take node 1 (lowest host_id) out of the live set.
        ring.set_node_state(1, NodeState::Leaving);

        let n2 = select_initiated_ranges(&ring, 2, 3);
        let n3 = select_initiated_ranges(&ring, 3, 3);

        assert!(
            !n2.is_empty(),
            "node 2 (now lowest live host_id) must take over initiation"
        );
        assert!(n3.is_empty(), "node 3 still defers to node 2");
        // The down node is never a repair peer.
        for r in &n2 {
            assert!(!r.peers.contains(&1), "down node must not be a peer");
        }
    }

    /// Single-node ring (or RF=1): no peers to reconcile → initiate nothing.
    #[test]
    fn single_node_selects_nothing() {
        let mut ring = TokenRing::new();
        ring.add_node(1, node("10.0.0.1:7000", 1, NodeState::Normal));
        ring.assign_tokens(1, &[100, 400, 700]);
        assert!(select_initiated_ranges(&ring, 1, 1).is_empty());
        assert!(select_initiated_ranges(&ring, 1, 3).is_empty());
    }

    /// A node not yet in the ring initiates nothing (no host_id to compare).
    #[test]
    fn unknown_local_node_selects_nothing() {
        let ring = three_node_ring();
        assert!(select_initiated_ranges(&ring, 999, 3).is_empty());
    }
}
