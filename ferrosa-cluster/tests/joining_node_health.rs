//! TDD RED: **a stuck `Joining` member must not be silent.**
//!
//! ## Observed production failure (local 3-node memory cluster, 2026-10-02)
//!
//! After a rolling restart, node2 came back as a full CQL-serving member with
//! `peers=2`, yet two things went wrong *silently*:
//!
//! 1. `SELECT ... FROM agent_memory.entity_store` (paged full walk) returned
//!    one FEWER row on node2 than on node1/node3, reproducibly — while the
//!    "missing" row was still reachable by primary key on all three nodes.
//! 2. `ferrosa-ctl repair --keyspace agent_memory --table entity_store --rf 3`
//!    run *from node2* reported `sessions_ok: 0, sessions_total: 0` — node2
//!    computed **no owned token ranges at all**.
//!
//! Both symptoms have ONE root cause: node2 is stuck in `NodeState::Joining`
//! (all three nodes' ring views agree). `TokenRing::replicas()` skips any
//! node that is not `Normal`/`Learner{owns_tokens:true}`, so a Joining
//! token-owner is dropped from replica sets, and `owned_token_ranges()`
//! (which asks `replicas()`) yields nothing.
//!
//! Nothing surfaced this: the ring endpoint *does* report the state (that is
//! how it was found), but no health signal, metric, or log says "a member is
//! not Normal". The cluster looked healthy while serving a degraded ring.
//!
//! ## Invariants this change must hold
//!
//! - **INV-A (observability)** — if any ring member is not `Normal`
//!   (i.e. `Joining` / `Leaving` / `Decommissioned`), cluster ring health
//!   MUST report the cluster as non-normal. Silence is the defect.
//! - **INV-A′ (non-vacuity)** — an all-`Normal` ring MUST report healthy, so
//!   the signal does not degrade into noise.
//! - **INV-B (no silent misroute)** — when a token-owning member is excluded
//!   from `replicas()` because of its state, that exclusion MUST be
//!   detectable; callers can then refuse to serve a partial/degraded replica
//!   set instead of silently serving the wrong node.
//! - **INV-C (promotion is planned, not hoped)** — the Promote phase must
//!   yield exactly one `RaftOp::SetNodeState { Normal }` for each
//!   token-holding member still stuck in `Joining`, and none when every
//!   member is already `Normal` (no needless churn).
//!
//! RED expectation for this file: it fails to COMPILE because
//! `ring_health`, `RingHealth`, `ring_data_scatter_risk`, and
//! `promote_joining_members` do not exist yet. That is the correct red —
//! the capability is absent.

use ferrosa_cluster::raft::{NodeInfo, NodeState, RaftOp};
use ferrosa_cluster::repair::coordinator::{
    joiners_awaiting_bootstrap, promote_joining_members, ring_data_scatter_risk, ring_health,
    RingHealth,
};
use ferrosa_cluster::ring::TokenRing;

fn node(addr: &str, state: NodeState) -> NodeInfo {
    NodeInfo {
        host_id: uuid::Uuid::new_v4(),
        addr: addr.into(),
        data_center: "dc1".into(),
        rack: "r1".into(),
        state,
        cql_broadcast: None,
    }
}

/// node1 Normal, node2 **Joining**, node3 Normal — the exact production shape.
fn ring_with_joining_node2() -> TokenRing {
    let mut ring = TokenRing::new();
    ring.add_node(1, node("n1:7000", NodeState::Normal));
    ring.add_node(2, node("n2:7000", NodeState::Joining));
    ring.add_node(3, node("n3:7000", NodeState::Normal));
    ring.assign_tokens(1, &[0, 100, 200]);
    ring.assign_tokens(2, &[50, 150, 250]);
    ring.assign_tokens(3, &[75, 175, 275]);
    ring
}

fn all_normal_ring() -> TokenRing {
    let mut ring = TokenRing::new();
    ring.add_node(1, node("n1:7000", NodeState::Normal));
    ring.add_node(2, node("n2:7000", NodeState::Normal));
    ring.add_node(3, node("n3:7000", NodeState::Normal));
    ring.assign_tokens(1, &[0, 100, 200]);
    ring.assign_tokens(2, &[50, 150, 250]);
    ring.assign_tokens(3, &[75, 175, 275]);
    ring
}

// ---------------------------------------------------------------- INV-A

/// Given a ring where one member is stuck `Joining` while its peers are
/// `Normal`, when ring health is computed, then the cluster must be reported
/// NON-normal and the offending member named.
#[test]
fn joining_member_makes_the_ring_report_non_normal() {
    let ring = ring_with_joining_node2();

    let health = ring_health(&ring);

    assert!(
        health.has_non_normal_members(),
        "a Joining peer silently served as a healthy ring — this is the bug. \
         health={health:?}"
    );
    assert_eq!(
        health.non_normal_members,
        vec![(2, NodeState::Joining)],
        "the stuck Joining member must be identified by node_id, not just counted"
    );
    assert!(
        !health.is_healthy(),
        "cluster with a Joining member must not satisfy is_healthy()"
    );
}

/// INV-A′ non-vacuity: the signal must not fire on a healthy cluster, or it
/// degrades into noise every operator learns to ignore.
#[test]
fn all_normal_ring_reports_healthy() {
    let ring = all_normal_ring();

    let health: RingHealth = ring_health(&ring);

    assert!(
        !health.has_non_normal_members(),
        "an all-Normal ring reported non-normal members: {health:?}"
    );
    assert!(health.is_healthy(), "an all-Normal ring must be healthy");
    assert_eq!(health.total_members, 3, "member count must be exact");
}

// ---------------------------------------------------------------- INV-B

/// Given a ring where a **token-owning** member is `Joining`, when replica
/// selection runs for a token in that member's range, then the member is
/// silently dropped from the replica set — and that misroute must be
/// detectable rather than invisible.
#[test]
fn joining_token_owner_is_detectably_misrouted() {
    let ring = ring_with_joining_node2();

    // Token 60 falls in node2's own range (node2 holds token 50), so node2 is
    // the correct owner — but it is Joining, so replicas() drops it.
    let replicas = ring.replicas(60, 1);
    assert!(
        !replicas.contains(&2),
        "precondition: a Joining owner is excluded from replicas() (got {replicas:?})"
    );

    let risk = ring_data_scatter_risk(&ring);

    assert!(
        risk.contains(&2),
        "node2 owns tokens but is excluded from replica sets — this silent \
         misroute must be reported, got {risk:?}"
    );
}

/// INV-B non-vacuity: no state-based exclusions ⇒ no scatter risk reported.
#[test]
fn all_normal_ring_has_no_data_scatter_risk() {
    let ring = all_normal_ring();

    let risk = ring_data_scatter_risk(&ring);

    assert!(
        risk.is_empty(),
        "a fully-Normal ring must report no scatter risk, got {risk:?}"
    );
}

// ---------------------------------------------------------------- INV-C

/// Given a cluster with a `Joining` member that holds tokens, when the Promote
/// phase is planned from the member-state view, then it must yield exactly one
/// `SetNodeState { Normal }` for that member.
#[test]
fn promote_plan_emits_one_promotion_per_joining_member() {
    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, NodeState::Normal);
    members.insert(2u64, NodeState::Joining);
    members.insert(3u64, NodeState::Normal);

    // Node 2's bootstrap completed and was recorded (P0-4).
    let plan = promote_joining_members(&members, &[2].into());

    assert_eq!(
        plan.len(),
        1,
        "exactly one member needs promotion: {plan:?}"
    );
    match &plan[0] {
        RaftOp::SetNodeState { node_id, state } => {
            assert_eq!(*node_id, 2, "the stuck Joining member is the target");
            assert_eq!(*state, NodeState::Normal, "and it is promoted to Normal");
        }
        other => panic!("expected SetNodeState{{Normal}} for node 2, got {other:?}"),
    }
}

/// INV-C non-vacuity: an all-Normal cluster must produce an empty plan so the
/// Promote phase does not churn Raft state on every pass.
#[test]
fn promote_plan_is_empty_when_all_members_normal() {
    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, NodeState::Normal);
    members.insert(2u64, NodeState::Normal);
    members.insert(3u64, NodeState::Normal);

    let plan = promote_joining_members(&members, &Default::default());

    assert!(
        plan.is_empty(),
        "no promotions expected for an all-Normal cluster, got {plan:?}"
    );
}

/// INV-C boundary: `Leaving` / `Decommissioned` are deliberate OPERATOR
/// intent. A recovery pass that "promotes everything not Normal back to
/// Normal" would resurrect a node an operator is draining or has removed —
/// data would start landing on it again. The plan must touch `Joining` only.
#[test]
fn promote_plan_does_not_reverse_operator_intent() {
    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, NodeState::Normal);
    members.insert(2u64, NodeState::Leaving);
    members.insert(3u64, NodeState::Decommissioned);

    let plan = promote_joining_members(&members, &Default::default());

    assert!(
        plan.is_empty(),
        "a draining or removed member must NOT be promoted back to Normal: {plan:?}"
    );
}

/// INV-C boundary: a learner is a distinct long-lived replica state machine
/// (ADR-014), not a joiner. Forcing it to `Normal` would give it a vote it was
/// deliberately never granted.
#[test]
fn promote_plan_does_not_promote_learners() {
    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, NodeState::Normal);
    members.insert(2u64, NodeState::Learner { owns_tokens: true });

    let plan = promote_joining_members(&members, &Default::default());

    assert!(
        plan.is_empty(),
        "a learner is not a joiner and must not be promoted to Normal: {plan:?}"
    );
}

/// The production failure, end to end at the decision level: a recovered ring
/// with a stuck `Joining` member must be BOTH reported unhealthy AND
/// repairable — reporting without a way out is not a fix, and a way out
/// without a report is how this stayed hidden.
#[test]
fn recovered_ring_with_a_stuck_joiner_is_reported_and_repairable() {
    fn member_states(ring: &TokenRing) -> std::collections::BTreeMap<u64, NodeState> {
        ring.node_ids()
            .into_iter()
            .filter_map(|id| ring.get_node(id).map(|i| (id, i.state)))
            .collect()
    }

    let ring = ring_with_joining_node2();

    let health = ring_health(&ring);
    assert!(
        !health.is_healthy(),
        "the degraded ring must report unhealthy: {health:?}"
    );

    // The joiner's bootstrap record committed; only its promotion was lost.
    let plan = promote_joining_members(&member_states(&ring), &[2].into());
    assert_eq!(plan.len(), 1, "exactly the stuck joiner is repairable");
    match &plan[0] {
        RaftOp::SetNodeState { node_id, state } => {
            assert_eq!(*node_id, 2, "node2 is the stuck joiner");
            assert_eq!(*state, NodeState::Normal, "and it is promoted to Normal");
        }
        other => panic!("expected SetNodeState{{Normal}} for node 2, got {other:?}"),
    }
}

// ---------------------------------------------------------------- P0-4

/// P0-4: a `Joining` member whose bootstrap never recorded completion must NOT
/// be promoted by the restart pass. Before the fix every `Joining` member was
/// promoted, including one whose stream a restart cut off, so it became a
/// replica without its data and served short reads.
#[test]
fn promote_plan_skips_a_joiner_without_a_bootstrap_record() {
    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, NodeState::Normal);
    members.insert(2u64, NodeState::Joining);
    members.insert(3u64, NodeState::Joining);

    // Only node 3 finished and recorded its bootstrap.
    let recorded = [3u64].into();
    let plan = promote_joining_members(&members, &recorded);

    let promoted: Vec<u64> = plan
        .iter()
        .map(|op| match op {
            RaftOp::SetNodeState {
                node_id,
                state: NodeState::Normal,
            } => *node_id,
            other => panic!("unexpected op {other:?}"),
        })
        .collect();
    assert_eq!(
        promoted,
        vec![3],
        "only the joiner with a bootstrap record may be promoted"
    );
    assert_eq!(
        joiners_awaiting_bootstrap(&members, &recorded),
        vec![2],
        "the unrecorded joiner must be reported as awaiting bootstrap"
    );
}

/// P0-4 migration: a cluster upgraded from a build with no records must not
/// get stuck. Members already `Normal` are grandfathered: they need no record,
/// are never demoted, and are never reported as awaiting bootstrap.
#[test]
fn normal_members_are_grandfathered_without_records() {
    let mut members = std::collections::BTreeMap::new();
    members.insert(1u64, NodeState::Normal);
    members.insert(2u64, NodeState::Normal);
    members.insert(3u64, NodeState::Normal);
    let none = std::collections::BTreeSet::new();

    assert!(promote_joining_members(&members, &none).is_empty());
    assert!(
        joiners_awaiting_bootstrap(&members, &none).is_empty(),
        "grandfathered Normal members must not be flagged"
    );
}
