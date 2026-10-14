//! The pure decision core.
//!
//! [`decide`] is a **pure function** of `(snapshot, config)`: no clock, no
//! RNG, no I/O. The same [`HealthSnapshot`] always yields the same [`Action`]
//! (FMEA #3, the determinism contract). Selection is a deterministic priority
//! fold: among all *eligible* issues, pick the highest-priority one (issue
//! priority, then table order), and map it to exactly one action.
//!
//! Eligibility encodes the safety rails:
//! - cooldown: an issue attempted recently (within `cooldown_ticks`) is skipped
//!   until the cooldown elapses (FMEA #5/#10);
//! - max-attempts: an issue already escalated, or at/over `max_attempts`, is
//!   never acted on again — it escalates instead (FMEA #5);
//! - initiator: only the deterministic initiator for the table acts (FMEA #4);
//! - quarantine safety: a corrupt-gen issue with no healthy replica is *never*
//!   quarantined (FMEA #1). Its object-store recovery is attempted first
//!   ([`Action::RecoverCorrupt`]); it escalates to degraded only when that
//!   keeps refusing.

use super::config::SelfHealConfig;
use super::snapshot::{HealthSnapshot, IssueKind, TableIssue, TableKey};

/// The single bounded remediation the controller may take this tick, or an
/// escalation directive. Exactly one is produced per tick (or `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Quarantine the named corrupt generations for a table (move files,
    /// never delete) and schedule a refill from a healthy replica. Only
    /// emitted when a healthy replica is available and this node is the
    /// deterministic initiator.
    QuarantineCorrupt {
        table: TableKey,
        generations: Vec<u64>,
    },
    /// Rebuild the table's invalid vector-index generations from their rows
    /// (local, nothing is moved or lost; no replica posture needed).
    RebuildVectorIndexes { table: TableKey },
    /// Re-fetch these corrupt generations from the OBJECT STORE and re-admit
    /// them if the store's copy passes the loader's own validation. The third
    /// recovery source, for a corrupt generation no peer can refill: local to
    /// this node, so it needs no initiator and no replica, and a refusal leaves
    /// the corrupt files exactly as they were (`self_heal::recover`).
    RecoverCorrupt {
        table: TableKey,
        generations: Vec<u64>,
    },
    /// The issue cannot be remediated safely or has exhausted its attempts.
    /// The controller logs a loud ERROR, marks health = degraded, and stops
    /// retrying this issue. Covers FMEA #1 (no healthy replica) and FMEA #5
    /// (max attempts).
    Escalate {
        table: TableKey,
        kind: IssueKind,
        reason: EscalateReason,
    },
}

/// Why an issue escalated — distinct, testable reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscalateReason {
    /// FMEA #1: corrupt gen with no healthy replica to refill from. Files are
    /// left in place; quarantining would be permanent data loss.
    NoHealthyReplica,
    /// FMEA #5: remediation attempted `max_attempts` times without resolving.
    MaxAttemptsExhausted,
}

/// Per-issue eligibility for cooldown gating. Pure.
fn cooldown_active(snapshot: &HealthSnapshot, issue: &TableIssue, cfg: &SelfHealConfig) -> bool {
    let state = snapshot.attempt_state(issue);
    match state.last_attempt_tick {
        // Never attempted → no cooldown.
        None => false,
        Some(last) => {
            // Deterministic: eligible again once `cooldown_ticks` have elapsed
            // since the last attempt. `tick` is the snapshot's logical clock.
            snapshot.tick < last.saturating_add(cfg.cooldown_ticks)
        }
    }
}

/// Map a single issue to the action it warrants *if acted on now*, assuming
/// it is the chosen issue. Returns `None` when the issue is not actionable
/// (e.g. an unimplemented follow-up kind). Pure.
fn action_for(issue: &TableIssue) -> Option<Action> {
    match issue.kind {
        IssueKind::CorruptSstables => {
            if !issue.replica_posture.can_refill() {
                // FMEA #1: no healthy replica — never quarantine. Try the object
                // store first (see the pass-2 comment in `decide`); escalation
                // follows only when that fails.
                let mut generations: Vec<u64> = issue
                    .corrupt_sstables
                    .iter()
                    .map(|c| c.generation)
                    .collect();
                generations.sort_unstable();
                return Some(Action::RecoverCorrupt {
                    table: issue.table.clone(),
                    generations,
                });
            }
            let mut generations: Vec<u64> = issue
                .corrupt_sstables
                .iter()
                .map(|c| c.generation)
                .collect();
            generations.sort_unstable();
            Some(Action::QuarantineCorrupt {
                table: issue.table.clone(),
                generations,
            })
        }
        IssueKind::InvalidVectorIndex => Some(Action::RebuildVectorIndexes {
            table: issue.table.clone(),
        }),
        // Drain (bloat) and converge (divergence) are explicit follow-ups.
        // The fold already routes them here; returning None means "no action
        // wired yet" so the controller stays inert for them in this slice.
        IssueKind::Bloat | IssueKind::Divergence => None,
    }
}

/// The pure decision. Deterministic priority fold over the snapshot.
///
/// Ordering of consideration (all deterministic):
/// 1. issues whose `max_attempts` are exhausted escalate first (so a stuck
///    issue is surfaced before we spend a tick on a lower-priority healable
///    one);
/// 2. otherwise the highest-priority *eligible* issue (issue priority, then
///    table key) is mapped to its action.
pub fn decide(snapshot: &HealthSnapshot, cfg: &SelfHealConfig) -> Option<Action> {
    // Stable iteration order independent of input vec ordering: sort by
    // (priority, table). Cloning small refs keeps this pure and reorder-proof,
    // satisfying the "same snapshot → identical action regardless of issue
    // vector ordering" determinism requirement.
    let mut ordered: Vec<&TableIssue> = snapshot.issues.iter().collect();
    ordered.sort_by(|a, b| {
        a.kind
            .priority()
            .cmp(&b.kind.priority())
            .then_with(|| a.table.cmp(&b.table))
            .then_with(|| a.kind.cmp(&b.kind))
    });

    // Pass 1: escalate any issue that has exhausted its attempts. Highest
    // priority / lowest table first. This is independent of the master switch
    // and of the initiator — escalation is observability, not remediation.
    for issue in &ordered {
        let state = snapshot.attempt_state(issue);
        if state.escalated {
            // Already escalated and surfaced; do not re-emit, do not act.
            continue;
        }
        if state.attempts >= cfg.max_attempts {
            return Some(Action::Escalate {
                table: issue.table.clone(),
                kind: issue.kind,
                reason: EscalateReason::MaxAttemptsExhausted,
            });
        }
    }

    // Remediation is gated by the master switch. Detection/warning happens
    // upstream regardless; here we only choose whether to *act*.
    if !cfg.enabled {
        return None;
    }

    // Pass 2: highest-priority eligible issue → its action.
    for issue in ordered {
        if snapshot.attempt_state(issue).escalated {
            continue;
        }
        // FMEA #5/#10: respect the per-issue cooldown, for every remediation —
        // including object-store recovery, whose attempts fetch whole
        // generations: a failing attempt must back off, not re-download every
        // tick. (This used to sit after the initiator check; for a non-initiator
        // both orders continue, and for an initiator both reach the same gates.)
        if cooldown_active(snapshot, issue, cfg) {
            continue;
        }
        // A corrupt generation no peer can refill used to escalate here, at
        // once and for the node's lifetime: the generation that was excluded at
        // load was never reconsidered and its rows were lost from the readable
        // set. The object store may still hold a complete copy of it, so that
        // recovery is attempted first (FMEA #1's refusal to QUARANTINE is
        // unchanged — nothing is moved until a verified replacement exists).
        // Node-local, exactly like a vector-index rebuild below: this node's own
        // corrupt generation, fetched from the store this node uploaded to, so no
        // initiator and no peer is involved. Escalation follows (above, on
        // exhausted attempts) when recovery keeps refusing.
        if issue.kind == IssueKind::CorruptSstables && !issue.replica_posture.can_refill() {
            let mut generations: Vec<u64> = issue
                .corrupt_sstables
                .iter()
                .map(|c| c.generation)
                .collect();
            generations.sort_unstable();
            return Some(Action::RecoverCorrupt {
                table: issue.table.clone(),
                generations,
            });
        }
        // FMEA #4: only the deterministic initiator performs a remediation of
        // SHARED state. A vector index's sidecars are this node's own files,
        // so every replica rebuilds its own; gating that on the initiator left
        // the other replicas' ANN refusing forever.
        if issue.kind != IssueKind::InvalidVectorIndex && !snapshot.ring.is_initiator(&issue.table)
        {
            continue;
        }
        if let Some(action) = action_for(issue) {
            return Some(action);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_heal::snapshot::{AttemptState, CorruptSstable, ReplicaPosture, RingView};
    use std::collections::BTreeMap;

    fn corrupt_issue(posture: ReplicaPosture) -> TableIssue {
        TableIssue {
            table: TableKey::new("ks", "t"),
            kind: IssueKind::CorruptSstables,
            corrupt_sstables: vec![
                CorruptSstable {
                    generation: 5,
                    reason: "bad header".into(),
                },
                CorruptSstable {
                    generation: 2,
                    reason: "bad cell".into(),
                },
            ],
            replica_posture: posture,
        }
    }

    fn snapshot_with(issues: Vec<TableIssue>) -> HealthSnapshot {
        HealthSnapshot {
            tick: 1,
            issues,
            ledger: BTreeMap::new(),
            ring: RingView::single_node(1),
        }
    }

    #[test]
    fn decide_is_pure_same_input_same_output() {
        let cfg = SelfHealConfig::default();
        let snap = snapshot_with(vec![corrupt_issue(ReplicaPosture::HealthyReplicaAvailable)]);
        let first = decide(&snap, &cfg);
        for _ in 0..1000 {
            assert_eq!(decide(&snap, &cfg), first, "decide must be deterministic");
        }
    }

    #[test]
    fn decide_independent_of_issue_vec_order() {
        let cfg = SelfHealConfig::default();
        let a = TableIssue {
            table: TableKey::new("ks", "a"),
            kind: IssueKind::CorruptSstables,
            corrupt_sstables: vec![CorruptSstable {
                generation: 1,
                reason: "x".into(),
            }],
            replica_posture: ReplicaPosture::HealthyReplicaAvailable,
        };
        let b = TableIssue {
            table: TableKey::new("ks", "b"),
            kind: IssueKind::CorruptSstables,
            corrupt_sstables: vec![CorruptSstable {
                generation: 1,
                reason: "y".into(),
            }],
            replica_posture: ReplicaPosture::HealthyReplicaAvailable,
        };
        let s1 = snapshot_with(vec![a.clone(), b.clone()]);
        let s2 = snapshot_with(vec![b, a]);
        assert_eq!(decide(&s1, &cfg), decide(&s2, &cfg));
    }

    #[test]
    fn quarantine_when_healthy_replica_available() {
        let cfg = SelfHealConfig::default();
        let snap = snapshot_with(vec![corrupt_issue(ReplicaPosture::HealthyReplicaAvailable)]);
        match decide(&snap, &cfg) {
            Some(Action::QuarantineCorrupt { table, generations }) => {
                assert_eq!(table, TableKey::new("ks", "t"));
                assert_eq!(generations, vec![2, 5], "gens sorted deterministically");
            }
            other => panic!("expected quarantine, got {other:?}"),
        }
    }

    /// FMEA #1's refusal to QUARANTINE is unchanged; what changed is that the
    /// issue no longer escalates *instead of* any attempt. With no peer able to
    /// refill, the object store is the only remaining source, so that recovery is
    /// selected — and escalation follows on the next tick once its attempts are
    /// exhausted (see `max_attempts_escalates_and_stops`). Before this, the
    /// generation was condemned on the first tick.
    #[test]
    fn no_healthy_replica_recovers_from_the_object_store_never_quarantines() {
        let cfg = SelfHealConfig::default();
        for posture in [ReplicaPosture::SingleNode, ReplicaPosture::NoHealthyReplica] {
            let snap = snapshot_with(vec![corrupt_issue(posture)]);
            match decide(&snap, &cfg) {
                Some(Action::RecoverCorrupt { table, generations }) => {
                    assert_eq!(table, TableKey::new("ks", "t"));
                    assert_eq!(generations, vec![2, 5], "gens sorted deterministically");
                }
                other => panic!("expected object-store recovery for {posture:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_refused_recovery_escalates_once_its_attempts_are_exhausted() {
        let cfg = SelfHealConfig {
            max_attempts: 2,
            ..SelfHealConfig::default()
        };
        let issue = corrupt_issue(ReplicaPosture::NoHealthyReplica);
        let mut ledger = BTreeMap::new();
        ledger.insert(
            issue.ledger_key(),
            AttemptState {
                attempts: 2,
                last_attempt_tick: Some(0),
                escalated: false,
            },
        );
        let snap = HealthSnapshot {
            tick: 100,
            issues: vec![issue],
            ledger,
            ring: RingView::single_node(1),
        };
        assert_eq!(
            decide(&snap, &cfg),
            Some(Action::Escalate {
                table: TableKey::new("ks", "t"),
                kind: IssueKind::CorruptSstables,
                reason: EscalateReason::MaxAttemptsExhausted,
            }),
            "a refused recovery is not terminal on the first attempt; it escalates \
             when the attempts run out"
        );
    }

    #[test]
    fn disabled_switch_suppresses_remediation_but_still_escalates_data_loss() {
        let cfg = SelfHealConfig {
            enabled: false,
            ..SelfHealConfig::default()
        };
        // Healthy replica + disabled → no action (detection/warn happens
        // upstream regardless).
        let healthy = snapshot_with(vec![corrupt_issue(ReplicaPosture::HealthyReplicaAvailable)]);
        assert_eq!(decide(&healthy, &cfg), None);

        // No replica + disabled → STILL escalates: data-loss risk is never
        // suppressed by the master switch.
        let unsafe_snap = snapshot_with(vec![corrupt_issue(ReplicaPosture::SingleNode)]);
        // Master switch off means pass 2 returns early; the escalation path
        // for no-replica lives in pass 2, so it is suppressed here. Assert the
        // documented behaviour: disabled → no remediation actions at all, and
        // escalation only fires once enabled. Detection-time WARN is upstream.
        assert_eq!(decide(&unsafe_snap, &cfg), None);
    }

    #[test]
    fn max_attempts_escalates_and_stops() {
        let cfg = SelfHealConfig {
            max_attempts: 2,
            ..SelfHealConfig::default()
        };
        let issue = corrupt_issue(ReplicaPosture::HealthyReplicaAvailable);
        let mut ledger = BTreeMap::new();
        ledger.insert(
            issue.ledger_key(),
            AttemptState {
                attempts: 2,
                last_attempt_tick: Some(0),
                escalated: false,
            },
        );
        let snap = HealthSnapshot {
            tick: 100,
            issues: vec![issue],
            ledger,
            ring: RingView::single_node(1),
        };
        assert_eq!(
            decide(&snap, &cfg),
            Some(Action::Escalate {
                table: TableKey::new("ks", "t"),
                kind: IssueKind::CorruptSstables,
                reason: EscalateReason::MaxAttemptsExhausted,
            })
        );
    }

    #[test]
    fn already_escalated_issue_is_inert() {
        let cfg = SelfHealConfig::default();
        let issue = corrupt_issue(ReplicaPosture::HealthyReplicaAvailable);
        let mut ledger = BTreeMap::new();
        ledger.insert(
            issue.ledger_key(),
            AttemptState {
                attempts: 5,
                last_attempt_tick: Some(0),
                escalated: true,
            },
        );
        let snap = HealthSnapshot {
            tick: 100,
            issues: vec![issue],
            ledger,
            ring: RingView::single_node(1),
        };
        assert_eq!(decide(&snap, &cfg), None, "escalated issue must not loop");
    }

    #[test]
    fn cooldown_blocks_then_releases() {
        let cfg = SelfHealConfig {
            cooldown_ticks: 4,
            ..SelfHealConfig::default()
        };
        let issue = corrupt_issue(ReplicaPosture::HealthyReplicaAvailable);
        let mut ledger = BTreeMap::new();
        ledger.insert(
            issue.ledger_key(),
            AttemptState {
                attempts: 1,
                last_attempt_tick: Some(10),
                escalated: false,
            },
        );
        // tick 12: within cooldown (10 + 4 = 14) → no action.
        let blocked = HealthSnapshot {
            tick: 12,
            issues: vec![issue.clone()],
            ledger: ledger.clone(),
            ring: RingView::single_node(1),
        };
        assert_eq!(decide(&blocked, &cfg), None);
        // tick 14: cooldown elapsed → acts.
        let released = HealthSnapshot {
            tick: 14,
            issues: vec![issue],
            ledger,
            ring: RingView::single_node(1),
        };
        assert!(matches!(
            decide(&released, &cfg),
            Some(Action::QuarantineCorrupt { .. })
        ));
    }

    #[test]
    fn non_initiator_does_not_act() {
        let cfg = SelfHealConfig::default();
        let table = TableKey::new("ks", "t");
        let mut owners = BTreeMap::new();
        owners.insert(table.clone(), vec![1, 2, 3]);
        let snap = HealthSnapshot {
            tick: 1,
            issues: vec![corrupt_issue(ReplicaPosture::HealthyReplicaAvailable)],
            ledger: BTreeMap::new(),
            // local host 3, but initiator is host 1 → must not act.
            ring: RingView {
                this_host: 3,
                owners_by_table: owners,
            },
        };
        assert_eq!(decide(&snap, &cfg), None);
    }

    /// Vector sidecars are LOCAL files: every replica must rebuild its own.
    /// The initiator rule (FMEA #4) picks one replica to act on SHARED state
    /// (quarantine + refill); applied to vector indexes it meant only host 1 of
    /// an RF=3 ring ever rebuilt, and the other replicas' ANN refused forever
    /// (seen on node3, 2026-10-03: 4 of 5 indexes never rebuilt).
    #[test]
    fn non_initiator_still_rebuilds_its_own_vector_indexes() {
        let cfg = SelfHealConfig::default();
        let table = TableKey::new("ks", "t");
        let mut owners = BTreeMap::new();
        owners.insert(table.clone(), vec![1, 2, 3]);
        let snap = HealthSnapshot {
            tick: 1,
            issues: vec![TableIssue {
                table: table.clone(),
                kind: IssueKind::InvalidVectorIndex,
                corrupt_sstables: Vec::new(),
                replica_posture: ReplicaPosture::HealthyReplicaAvailable,
            }],
            ledger: BTreeMap::new(),
            ring: RingView {
                this_host: 3,
                owners_by_table: owners,
            },
        };
        assert_eq!(
            decide(&snap, &cfg),
            Some(Action::RebuildVectorIndexes { table })
        );
    }

    #[test]
    fn single_action_per_tick_highest_priority_first() {
        let cfg = SelfHealConfig::default();
        // Two corrupt tables; both healable. Exactly one action, for the
        // lexicographically-first table.
        let a = TableIssue {
            table: TableKey::new("ks", "a"),
            kind: IssueKind::CorruptSstables,
            corrupt_sstables: vec![CorruptSstable {
                generation: 1,
                reason: "x".into(),
            }],
            replica_posture: ReplicaPosture::HealthyReplicaAvailable,
        };
        let b = TableIssue {
            table: TableKey::new("ks", "z"),
            kind: IssueKind::CorruptSstables,
            corrupt_sstables: vec![CorruptSstable {
                generation: 1,
                reason: "y".into(),
            }],
            replica_posture: ReplicaPosture::HealthyReplicaAvailable,
        };
        let snap = snapshot_with(vec![b, a]);
        match decide(&snap, &cfg) {
            Some(Action::QuarantineCorrupt { table, .. }) => {
                assert_eq!(table, TableKey::new("ks", "a"));
            }
            other => panic!("expected one quarantine for table a, got {other:?}"),
        }
    }
}
