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

use std::collections::HashSet;
use std::time::Duration;

use ferrosa_storage::TableId;

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

/// Deterministic configuration for the automatic repair scheduler. All values
/// come from env (fixed defaults otherwise) so behaviour is reproducible.
#[derive(Debug, Clone)]
pub struct AutoRepairConfig {
    /// Master switch. `FERROSA_AUTO_REPAIR_ENABLED` (default **on**).
    pub enabled: bool,
    /// Full-coverage period — every owned range is repaired once per interval,
    /// spread round-robin across tables. `FERROSA_AUTO_REPAIR_INTERVAL_SECS`
    /// (default 86400 = 24h).
    pub interval: Duration,
    /// Tables repaired per sub-tick (load shaping).
    /// `FERROSA_AUTO_REPAIR_MAX_CONCURRENT_TABLES` (default 1).
    pub max_concurrent_tables: usize,
    /// Keyspace name prefixes never auto-repaired (system/internal).
    /// `FERROSA_AUTO_REPAIR_SKIP_KEYSPACES` (comma-separated; default `system`).
    pub skip_keyspaces: Vec<String>,
}

impl Default for AutoRepairConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(86_400),
            max_concurrent_tables: 1,
            skip_keyspaces: vec!["system".to_string()],
        }
    }
}

impl AutoRepairConfig {
    /// Build from environment, falling back to [`Default`] for any unset/unparseable var.
    pub fn from_env() -> Self {
        let d = Self::default();
        let enabled = std::env::var("FERROSA_AUTO_REPAIR_ENABLED")
            .ok()
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"))
            .unwrap_or(d.enabled);
        let interval = std::env::var("FERROSA_AUTO_REPAIR_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&s| s > 0)
            .map(Duration::from_secs)
            .unwrap_or(d.interval);
        let max_concurrent_tables = std::env::var("FERROSA_AUTO_REPAIR_MAX_CONCURRENT_TABLES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(d.max_concurrent_tables);
        let skip_keyspaces = std::env::var("FERROSA_AUTO_REPAIR_SKIP_KEYSPACES")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or(d.skip_keyspaces);
        Self {
            enabled,
            interval,
            max_concurrent_tables,
            skip_keyspaces,
        }
    }

    /// True if `table`'s keyspace matches any skip prefix.
    pub fn is_skipped(&self, table: &TableId) -> bool {
        self.skip_keyspaces
            .iter()
            .any(|p| table.keyspace().starts_with(p.as_str()))
    }
}

/// Pick the next batch of tables to repair this sub-tick, round-robin from
/// `cursor`, skipping system/skip-listed keyspaces and tables already in flight.
/// Pure + deterministic. Returns `(tables_to_repair, next_cursor)`.
///
/// Round-robin (not always-from-zero) guarantees forward progress across the
/// whole table set within one `interval` even if early tables are persistently
/// in-flight, and bounds per-tick load to `max_concurrent` (FMEA #8 runaway
/// cadence).
pub fn select_tables_for_tick(
    tables: &[TableId],
    cursor: usize,
    max_concurrent: usize,
    in_flight: &HashSet<TableId>,
    cfg: &AutoRepairConfig,
) -> (Vec<TableId>, usize) {
    let n = tables.len();
    if n == 0 || max_concurrent == 0 {
        return (Vec::new(), 0);
    }
    let mut picked = Vec::new();
    let mut i = cursor % n;
    let mut scanned = 0usize;
    // Scan at most one full lap so we never spin on an all-skipped set.
    while picked.len() < max_concurrent && scanned < n {
        let t = &tables[i];
        if !cfg.is_skipped(t) && !in_flight.contains(t) {
            picked.push(t.clone());
        }
        i = (i + 1) % n;
        scanned += 1;
    }
    (picked, i)
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

    fn tid(ks: &str, t: &str) -> TableId {
        TableId::new(ks, t)
    }

    #[test]
    fn config_defaults_are_self_managing() {
        let c = AutoRepairConfig::default();
        assert!(c.enabled, "auto-repair on by default");
        assert_eq!(c.interval, Duration::from_secs(86_400), "24h default");
        assert_eq!(c.max_concurrent_tables, 1);
        assert!(c.is_skipped(&tid("system_auth", "roles")), "system* skipped");
        assert!(!c.is_skipped(&tid("app", "users")), "user keyspace not skipped");
    }

    #[test]
    fn select_tables_skips_system_and_in_flight_round_robin() {
        let cfg = AutoRepairConfig::default();
        let tables = vec![
            tid("system", "local"),
            tid("app", "t1"),
            tid("app", "t2"),
            tid("app", "t3"),
        ];
        let mut in_flight = HashSet::new();
        in_flight.insert(tid("app", "t1"));

        // max 2: skip system.local (system*) and app.t1 (in-flight) → t2, t3.
        let (picked, _next) = select_tables_for_tick(&tables, 0, 2, &in_flight, &cfg);
        assert_eq!(picked, vec![tid("app", "t2"), tid("app", "t3")]);
    }

    #[test]
    fn select_tables_round_robin_covers_all_over_ticks() {
        let cfg = AutoRepairConfig::default();
        let tables = vec![tid("app", "a"), tid("app", "b"), tid("app", "c")];
        let empty = HashSet::new();

        // One table per tick (default max_concurrent_tables=1).
        let (p0, c0) = select_tables_for_tick(&tables, 0, 1, &empty, &cfg);
        let (p1, c1) = select_tables_for_tick(&tables, c0, 1, &empty, &cfg);
        let (p2, _c2) = select_tables_for_tick(&tables, c1, 1, &empty, &cfg);

        let mut covered: Vec<TableId> = Vec::new();
        covered.extend(p0);
        covered.extend(p1);
        covered.extend(p2);
        covered.sort_by(|a, b| a.table().cmp(b.table()));
        assert_eq!(covered, tables, "three ticks cover every table exactly once");
    }

    #[test]
    fn select_tables_empty_or_all_skipped_is_safe() {
        let cfg = AutoRepairConfig::default();
        let empty = HashSet::new();
        assert!(select_tables_for_tick(&[], 0, 1, &empty, &cfg).0.is_empty());
        // All-system set → nothing picked, no infinite scan.
        let sys = vec![tid("system", "a"), tid("system_auth", "b")];
        assert!(select_tables_for_tick(&sys, 0, 4, &empty, &cfg).0.is_empty());
    }
}
