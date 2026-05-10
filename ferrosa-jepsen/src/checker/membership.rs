//! Structural-invariant checker for ferrosa membership snapshots.
//!
//! This module implements Sprint 2 W2.4: a post-run check that, given one
//! `MembershipSnapshot` per node in the cluster, verifies the six structural
//! invariants from `specs/raft-invariants.md` §B:
//!
//! - **I-06** Four-maps agree: `state.members ⟺ openraft.voters ⟺ node_map ⟺ peer_manager.peers`.
//! - **I-07** No empty addresses in `state.members`.
//! - **I-08** Every openraft voter is in `state.members`.
//! - **I-09** Every node in `state.members` is either an openraft voter or a learner.
//! - **I-10** No decommissioned host appears in any of the four maps.
//! - **I-13** Quorum sizing is committed (`committed_cluster_size`), not connected
//!   (`peer_manager.live_peers().count()`).
//!
//! The companion HTTP endpoint that produces these snapshots lives at
//! `/admin/membership-snapshot` (Sprint 2 W2.3); the orchestrator collects
//! one snapshot per node after a workload completes and feeds them here.
//!
//! ## Why these checks are valuable
//!
//! Six bugs in the recent genome lived in the four-maps-must-agree property.
//! A snapshot that satisfies I-06–I-10 + I-13 across every node is a strong
//! end-to-end witness that no map drifted during the run.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Membership view as exposed by the `/admin/membership-snapshot` endpoint.
///
/// All four maps key off the same logical identity (the node's `host_id`,
/// represented as a string for transport) so the check functions can compare
/// them without needing the node-id ↔ host-id translation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipSnapshot {
    /// The reporter's own host id — used for diagnostics in violation reports.
    pub reporter_host_id: String,

    /// `state.members`: `host_id -> NodeView`.
    pub state_members: BTreeMap<String, NodeView>,

    /// openraft `Membership.voters()`, projected to host_ids.
    pub openraft_voters: BTreeSet<String>,

    /// openraft learners, projected to host_ids. Empty when no learner is in flight.
    pub openraft_learners: BTreeSet<String>,

    /// `network_factory.node_map`: host_ids that have a routable client registered.
    pub node_map: BTreeSet<String>,

    /// `peer_manager.peers`: host_ids the peer manager knows about.
    pub peer_manager_peers: BTreeSet<String>,

    /// openraft's committed voter count — the source of truth for quorum sizing
    /// (I-13). Captured at snapshot time.
    pub committed_cluster_size: usize,

    /// `peer_manager.live_peers().count()` — the number of currently-connected
    /// peers. I-13 says this MUST NOT be used for quorum.
    pub live_peer_count: usize,
}

/// Lightweight projection of `state_machine::NodeInfo` for snapshot transport.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeView {
    pub host_id: String,
    pub addr: String,
    pub state: NodeStateLabel,
}

/// Subset of `NodeState` we care about for invariant checks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStateLabel {
    #[default]
    Joining,
    Normal,
    Leaving,
    Decommissioned,
    Other,
}

/// A single invariant violation. Aggregated by `check_membership_invariants`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvariantViolation {
    /// The invariant id (e.g. `"I-06"`).
    pub invariant: String,
    /// Reporter (host_id) on which the violation was observed. Empty when the
    /// violation is cluster-wide rather than per-node.
    pub reporter: String,
    /// Human-readable explanation including the offending host_id.
    pub message: String,
}

/// Run all six structural invariants against the provided snapshots.
///
/// Returns one violation per offending case. An empty vec means every
/// snapshot passes every check.
///
/// Runs both per-snapshot checks (I-06–I-10, I-13 each interpreted within a
/// single reporter's view) and cross-snapshot checks (the same maps, compared
/// across reporters — the bug class addressed by Sprint 1's silent-drop fix).
pub fn check_membership_invariants(snapshots: &[MembershipSnapshot]) -> Vec<InvariantViolation> {
    let mut violations = Vec::new();

    for snap in snapshots {
        violations.extend(check_i06_four_maps_agree(snap));
        violations.extend(check_i07_no_empty_addresses(snap));
        violations.extend(check_i08_openraft_subset_state_members(snap));
        violations.extend(check_i09_state_members_subset_voters_or_learners(snap));
        violations.extend(check_i10_no_decommissioned_in_any_map(snap));
        violations.extend(check_i13_quorum_sized_by_committed_voters(snap));
    }

    violations.extend(check_membership_cross_snapshot(snapshots));

    violations
}

/// I-06 (cross-snapshot variant): every pair of reporters must see the same
/// `state.members` and `openraft.voters`.
///
/// This catches the bug class where a non-leader silently drops a membership
/// proposal: the leader applies the change, the follower does not, and the
/// two snapshots diverge on the affected host_id.
pub fn check_membership_cross_snapshot(
    snapshots: &[MembershipSnapshot],
) -> Vec<InvariantViolation> {
    if snapshots.len() < 2 {
        return Vec::new();
    }

    let mut out = Vec::new();
    let canonical = &snapshots[0];
    let canonical_state: BTreeSet<String> = canonical.state_members.keys().cloned().collect();

    for other in &snapshots[1..] {
        let other_state: BTreeSet<String> = other.state_members.keys().cloned().collect();
        for missing in canonical_state.difference(&other_state) {
            out.push(InvariantViolation {
                invariant: "I-06".to_string(),
                reporter: other.reporter_host_id.clone(),
                message: format!(
                    "host {missing}: cross-snapshot drift — present on reporter {} \
                     but missing from reporter {}",
                    canonical.reporter_host_id, other.reporter_host_id
                ),
            });
        }
        for extra in other_state.difference(&canonical_state) {
            out.push(InvariantViolation {
                invariant: "I-06".to_string(),
                reporter: other.reporter_host_id.clone(),
                message: format!(
                    "host {extra}: cross-snapshot drift — present on reporter {} \
                     but missing from reporter {}",
                    other.reporter_host_id, canonical.reporter_host_id
                ),
            });
        }

        // Compare voter sets too.
        if canonical.openraft_voters != other.openraft_voters {
            out.push(InvariantViolation {
                invariant: "I-06".to_string(),
                reporter: other.reporter_host_id.clone(),
                message: format!(
                    "openraft.voters drift between reporter {} ({:?}) and reporter {} ({:?})",
                    canonical.reporter_host_id,
                    canonical.openraft_voters,
                    other.reporter_host_id,
                    other.openraft_voters
                ),
            });
        }
    }

    out
}

/// I-06: For every host_id `H` known to the cluster,
/// `state.members.contains(H) ⟺ openraft.voters.contains(H) ⟺ node_map.contains(H) ⟺ peer_manager.peers.contains(H)`.
///
/// Learners are excluded — they are deliberately in `state.members` but not
/// in `openraft.voters`. I-09 covers that boundary.
fn check_i06_four_maps_agree(snap: &MembershipSnapshot) -> Vec<InvariantViolation> {
    let mut out = Vec::new();
    let learners = &snap.openraft_learners;

    let state_members: BTreeSet<String> = snap.state_members.keys().cloned().collect();

    // Universe = union of all four maps minus learners (learners are I-09's job).
    let mut universe: BTreeSet<String> = BTreeSet::new();
    universe.extend(state_members.iter().cloned());
    universe.extend(snap.openraft_voters.iter().cloned());
    universe.extend(snap.node_map.iter().cloned());
    universe.extend(snap.peer_manager_peers.iter().cloned());

    for host in universe.difference(learners) {
        let in_state = state_members.contains(host);
        let in_voters = snap.openraft_voters.contains(host);
        let in_node_map = snap.node_map.contains(host);
        let in_peer_manager = snap.peer_manager_peers.contains(host);

        if !(in_state == in_voters && in_voters == in_node_map && in_node_map == in_peer_manager) {
            out.push(InvariantViolation {
                invariant: "I-06".to_string(),
                reporter: snap.reporter_host_id.clone(),
                message: format!(
                    "host {host}: four-maps drift on reporter {} — \
                     state_members={in_state} openraft_voters={in_voters} \
                     node_map={in_node_map} peer_manager={in_peer_manager}",
                    snap.reporter_host_id
                ),
            });
        }
    }

    out
}

/// I-07: Every node in `state.members` has a non-empty `addr`.
fn check_i07_no_empty_addresses(snap: &MembershipSnapshot) -> Vec<InvariantViolation> {
    snap.state_members
        .iter()
        .filter(|(_, view)| view.addr.is_empty())
        .map(|(host, _)| InvariantViolation {
            invariant: "I-07".to_string(),
            reporter: snap.reporter_host_id.clone(),
            message: format!(
                "host {host} on reporter {}: state.members entry has empty addr",
                snap.reporter_host_id
            ),
        })
        .collect()
}

/// I-08: `openraft.voters ⊆ state.members`.
fn check_i08_openraft_subset_state_members(snap: &MembershipSnapshot) -> Vec<InvariantViolation> {
    snap.openraft_voters
        .iter()
        .filter(|h| !snap.state_members.contains_key(h.as_str()))
        .map(|h| InvariantViolation {
            invariant: "I-08".to_string(),
            reporter: snap.reporter_host_id.clone(),
            message: format!(
                "host {h} on reporter {}: openraft voter is missing from state.members",
                snap.reporter_host_id
            ),
        })
        .collect()
}

/// I-09: Every host in `state.members` is either an openraft voter OR a learner.
fn check_i09_state_members_subset_voters_or_learners(
    snap: &MembershipSnapshot,
) -> Vec<InvariantViolation> {
    snap.state_members
        .keys()
        .filter(|h| {
            !snap.openraft_voters.contains(h.as_str())
                && !snap.openraft_learners.contains(h.as_str())
        })
        .map(|h| InvariantViolation {
            invariant: "I-09".to_string(),
            reporter: snap.reporter_host_id.clone(),
            message: format!(
                "host {h} on reporter {}: state.members entry is neither voter nor learner",
                snap.reporter_host_id
            ),
        })
        .collect()
}

/// I-10: Decommissioned hosts must not appear in any of the four maps.
///
/// The check is local to each snapshot: if `state.members` reports a host
/// in `Decommissioned` state, that's a violation regardless of the other maps;
/// likewise, if a host is in any of `openraft.voters | node_map | peer_manager`
/// while being marked decommissioned in `state.members`, that's a violation.
fn check_i10_no_decommissioned_in_any_map(snap: &MembershipSnapshot) -> Vec<InvariantViolation> {
    let mut out = Vec::new();
    for (host, view) in &snap.state_members {
        if !matches!(view.state, NodeStateLabel::Decommissioned) {
            continue;
        }
        // The host is decommissioned according to state.members. It MUST be gone
        // from every map (including state.members itself).
        out.push(InvariantViolation {
            invariant: "I-10".to_string(),
            reporter: snap.reporter_host_id.clone(),
            message: format!(
                "host {host} on reporter {}: state.members still lists a decommissioned node",
                snap.reporter_host_id
            ),
        });
        if snap.openraft_voters.contains(host) {
            out.push(InvariantViolation {
                invariant: "I-10".to_string(),
                reporter: snap.reporter_host_id.clone(),
                message: format!(
                    "host {host} on reporter {}: decommissioned host still in openraft.voters",
                    snap.reporter_host_id
                ),
            });
        }
        if snap.node_map.contains(host) {
            out.push(InvariantViolation {
                invariant: "I-10".to_string(),
                reporter: snap.reporter_host_id.clone(),
                message: format!(
                    "host {host} on reporter {}: decommissioned host still in node_map",
                    snap.reporter_host_id
                ),
            });
        }
        if snap.peer_manager_peers.contains(host) {
            out.push(InvariantViolation {
                invariant: "I-10".to_string(),
                reporter: snap.reporter_host_id.clone(),
                message: format!(
                    "host {host} on reporter {}: decommissioned host still in peer_manager.peers",
                    snap.reporter_host_id
                ),
            });
        }
    }
    out
}

/// I-13: `committed_cluster_size` must equal the number of openraft voters.
/// This catches code paths that derive quorum from `live_peers().count()`.
fn check_i13_quorum_sized_by_committed_voters(
    snap: &MembershipSnapshot,
) -> Vec<InvariantViolation> {
    let mut out = Vec::new();
    if snap.committed_cluster_size != snap.openraft_voters.len() {
        out.push(InvariantViolation {
            invariant: "I-13".to_string(),
            reporter: snap.reporter_host_id.clone(),
            message: format!(
                "reporter {}: committed_cluster_size={} but openraft.voters.len()={}",
                snap.reporter_host_id,
                snap.committed_cluster_size,
                snap.openraft_voters.len()
            ),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voter(host_id: &str, addr: &str) -> (String, NodeView) {
        (
            host_id.to_string(),
            NodeView {
                host_id: host_id.to_string(),
                addr: addr.to_string(),
                state: NodeStateLabel::Normal,
            },
        )
    }

    /// Build a clean 3-node snapshot reported by node1.
    fn clean_3node_snapshot(reporter: &str) -> MembershipSnapshot {
        let voters: BTreeSet<String> = ["node1", "node2", "node3"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let state_members: BTreeMap<String, NodeView> = [
            voter("node1", "node1:7000"),
            voter("node2", "node2:7000"),
            voter("node3", "node3:7000"),
        ]
        .into_iter()
        .collect();

        MembershipSnapshot {
            reporter_host_id: reporter.into(),
            state_members,
            openraft_voters: voters.clone(),
            openraft_learners: BTreeSet::new(),
            node_map: voters.clone(),
            peer_manager_peers: voters.clone(),
            committed_cluster_size: 3,
            live_peer_count: 3,
        }
    }

    /// W2.4 happy path: all six invariants hold on a clean 3-node snapshot.
    #[test]
    fn membership_snapshot_invariants_hold_on_clean_3_node_snapshot() {
        let snaps = vec![
            clean_3node_snapshot("node1"),
            clean_3node_snapshot("node2"),
            clean_3node_snapshot("node3"),
        ];
        let v = check_membership_invariants(&snaps);
        assert!(
            v.is_empty(),
            "clean 3-node cluster must satisfy all six invariants; got {v:#?}"
        );
    }

    /// I-06: drop a host from `node_map` and assert exactly one I-06 per snapshot.
    #[test]
    fn i06_violation_when_node_map_misses_a_voter() {
        let mut snap = clean_3node_snapshot("node1");
        snap.node_map.remove("node3");
        let v = check_membership_invariants(&[snap]);
        let i06: Vec<_> = v.iter().filter(|v| v.invariant == "I-06").collect();
        assert_eq!(
            i06.len(),
            1,
            "exactly one I-06 violation expected; got {v:#?}"
        );
        assert!(i06[0].message.contains("node3"));
    }

    /// I-07: empty addr in state.members is an I-07 violation.
    #[test]
    fn i07_violation_for_empty_addr() {
        let mut snap = clean_3node_snapshot("node1");
        snap.state_members.get_mut("node2").unwrap().addr = String::new();
        let v = check_membership_invariants(&[snap]);
        let i07: Vec<_> = v.iter().filter(|v| v.invariant == "I-07").collect();
        assert_eq!(i07.len(), 1);
        assert!(i07[0].message.contains("node2"));
    }

    /// I-08: openraft voter not in state.members.
    #[test]
    fn i08_violation_for_voter_missing_from_state_members() {
        let mut snap = clean_3node_snapshot("node1");
        snap.state_members.remove("node3");
        snap.node_map.remove("node3");
        snap.peer_manager_peers.remove("node3");
        // committed_cluster_size still 3 to isolate I-08; that creates an I-13 too,
        // so we filter for I-08 specifically.
        let v = check_membership_invariants(&[snap]);
        let i08: Vec<_> = v.iter().filter(|v| v.invariant == "I-08").collect();
        assert_eq!(i08.len(), 1);
        assert!(i08[0].message.contains("node3"));
    }

    /// I-09: state.members contains a host that is neither voter nor learner.
    #[test]
    fn i09_violation_for_state_member_not_in_voters_or_learners() {
        let mut snap = clean_3node_snapshot("node1");
        // Add a phantom node to state.members only.
        snap.state_members
            .insert("node4".into(), voter("node4", "node4:7000").1);
        // Don't add to voters or learners — that's the violation.
        // Add to other two maps so I-06 isn't triggered for node4.
        snap.node_map.insert("node4".into());
        snap.peer_manager_peers.insert("node4".into());
        // node4 still drifts vs voters → I-06 fires; we filter for I-09.
        let v = check_membership_invariants(&[snap]);
        let i09: Vec<_> = v.iter().filter(|v| v.invariant == "I-09").collect();
        assert_eq!(i09.len(), 1);
        assert!(i09[0].message.contains("node4"));
    }

    /// I-09: a learner in state.members but not voters is OK (transient).
    #[test]
    fn i09_no_violation_for_learner_in_state_members() {
        let mut snap = clean_3node_snapshot("node1");
        // Add node4 as a learner: present in state.members and learners.
        snap.state_members
            .insert("node4".into(), voter("node4", "node4:7000").1);
        snap.openraft_learners.insert("node4".into());
        snap.node_map.insert("node4".into());
        snap.peer_manager_peers.insert("node4".into());
        // Note: node4 in state_members but NOT in voters → still need to handle I-06.
        // I-06 ignores learners in its universe, so this should be clean for I-06 too.
        let v = check_membership_invariants(&[snap]);
        let i09: Vec<_> = v.iter().filter(|v| v.invariant == "I-09").collect();
        assert!(
            i09.is_empty(),
            "a learner in state.members should not trip I-09; got {v:#?}"
        );
    }

    /// I-10: a decommissioned host in state.members is a violation.
    #[test]
    fn i10_violation_for_decommissioned_in_state_members() {
        let mut snap = clean_3node_snapshot("node1");
        snap.state_members.get_mut("node3").unwrap().state = NodeStateLabel::Decommissioned;
        let v = check_membership_invariants(&[snap]);
        let i10: Vec<_> = v.iter().filter(|v| v.invariant == "I-10").collect();
        assert!(
            !i10.is_empty(),
            "decommissioned host should produce at least one I-10 violation; got {v:#?}"
        );
        assert!(i10.iter().any(|x| x.message.contains("node3")));
    }

    /// I-13: committed_cluster_size diverging from voters.len() is a violation.
    #[test]
    fn i13_violation_when_committed_size_mismatches_voters() {
        let mut snap = clean_3node_snapshot("node1");
        snap.committed_cluster_size = 2; // Pretend we recorded a connected count.
        let v = check_membership_invariants(&[snap]);
        let i13: Vec<_> = v.iter().filter(|v| v.invariant == "I-13").collect();
        assert_eq!(i13.len(), 1);
        assert!(i13[0].message.contains("committed_cluster_size=2"));
    }

    /// Snapshot serializes round-trip via JSON — required for the W2.3 endpoint.
    #[test]
    fn membership_snapshot_serializes_to_json() {
        let snap = clean_3node_snapshot("node1");
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("state_members"));
        assert!(json.contains("openraft_voters"));
        assert!(json.contains("node_map"));
        assert!(json.contains("peer_manager_peers"));
        let back: MembershipSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snap);
    }

    /// Empty input produces no violations.
    #[test]
    fn check_empty_snapshots_produces_no_violations() {
        let v = check_membership_invariants(&[]);
        assert!(v.is_empty());
    }

    // -----------------------------------------------------------------------
    // W2.5 — Cross-sprint regression test for the Sprint 1 silent-drop fix.
    //
    // Sprint 1 fixes a bug where a non-leader receiving a membership-mutating
    // RaftCommand silently dropped it instead of forwarding to the leader.
    // The symptom is a four-maps drift across nodes: the leader's view
    // reflects the change while the follower's view does not.
    //
    // We construct that drift synthetically and assert the cross-snapshot
    // check surfaces it as I-06. When Sprint 1 lands and the orchestrator
    // runs a real cluster, the same drift pattern is what Jepsen's smoke
    // tier should catch end-to-end (via /admin/membership-snapshot — W2.3).
    // -----------------------------------------------------------------------

    /// Lock the regression: if someone reverts Sprint 1's fix, the resulting
    /// cross-reporter drift must be flagged as I-06.
    #[test]
    fn membership_invariants_fail_on_silent_drop_revert() {
        // Leader's snapshot: full 3-node membership.
        let leader_snap = clean_3node_snapshot("node1");

        // Follower's snapshot after a silent-drop revert: it never received
        // the membership change for node3, so its four maps lack node3.
        let voters: BTreeSet<String> = ["node1", "node2"].iter().map(|s| s.to_string()).collect();
        let mut state_members: BTreeMap<String, NodeView> = BTreeMap::new();
        state_members.insert("node1".into(), voter("node1", "node1:7000").1);
        state_members.insert("node2".into(), voter("node2", "node2:7000").1);

        let follower_snap = MembershipSnapshot {
            reporter_host_id: "node2".into(),
            state_members,
            openraft_voters: voters.clone(),
            openraft_learners: BTreeSet::new(),
            node_map: voters.clone(),
            peer_manager_peers: voters,
            committed_cluster_size: 2,
            live_peer_count: 2,
        };

        let v = check_membership_invariants(&[leader_snap, follower_snap]);

        assert!(
            v.iter().any(|x| x.invariant == "I-06"),
            "silent-drop revert must produce an I-06 violation; got {v:#?}"
        );
        assert!(
            v.iter().any(|x| x.message.contains("node3")),
            "the violation must call out the missing host_id (node3); got {v:#?}"
        );
    }

    /// Cross-snapshot helper alone: with 0 or 1 snapshots, nothing to compare.
    #[test]
    fn cross_snapshot_check_no_op_for_one_snapshot() {
        let snap = clean_3node_snapshot("node1");
        let v = check_membership_cross_snapshot(&[snap]);
        assert!(v.is_empty());
        let v_empty = check_membership_cross_snapshot(&[]);
        assert!(v_empty.is_empty());
    }

    /// Cross-snapshot voter-set drift is reported even when state.members
    /// happens to align (a more subtle variant of the silent-drop bug).
    #[test]
    fn cross_snapshot_check_detects_voters_drift_alone() {
        let mut a = clean_3node_snapshot("node1");
        let mut b = clean_3node_snapshot("node2");
        // Voters drift but state.members is identical.
        b.openraft_voters.remove("node3");
        // Adjust b's committed_cluster_size so the per-snapshot I-13 doesn't
        // fire and dilute this assertion.
        b.committed_cluster_size = 2;
        // Also remove from the other 3 maps on b to keep its per-snapshot
        // I-06 clean — we want the cross-snapshot drift to be the sole signal.
        b.node_map.remove("node3");
        b.peer_manager_peers.remove("node3");
        // But state.members on b still has node3 → I-09 would fire on b
        // (state.members has a non-voter, non-learner). Remove it from b
        // state.members too so only voters drift remains.
        a.openraft_voters.insert("node3".into()); // already there in clean
        b.state_members.remove("node3");

        let v = check_membership_cross_snapshot(&[a, b]);
        assert!(
            v.iter().any(|x| x.message.contains("voters drift")),
            "cross-snapshot voter drift must be reported; got {v:#?}"
        );
    }
}
