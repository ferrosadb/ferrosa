//! Action execution: the quarantine remediation and the FMEA #1 safety rail.
//!
//! The pure [`decide`](super::decide::decide) core already refuses to *select*
//! a quarantine when no healthy replica can refill (it returns
//! [`Action::Escalate`] instead). This module performs the side effects for a
//! chosen action and **re-checks the safety invariant at the point of the
//! move** — defence in depth: even a mis-built snapshot cannot cause data
//! loss, because the executor independently verifies the posture before it
//! moves a single byte (FMEA #1).

use crate::engine::StorageEngine;

use super::decide::{Action, EscalateReason};
use super::metrics;
use super::snapshot::{ReplicaPosture, TableKey};

/// Outcome of executing one action — fed back into the controller's ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionOutcome {
    /// Quarantine succeeded: these generations were moved to `quarantine/`.
    /// A refill from a healthy replica is scheduled (hook, see below).
    Quarantined {
        table: TableKey,
        generations: Vec<u64>,
    },
    /// The action could not be performed safely or hit an error. The issue
    /// is escalated: health = degraded, files left in place.
    Escalated { table: TableKey, reason: String },
}

/// Resolve the on-disk table directory for an action. Injected so unit tests
/// can point at a fixture dir without a full cluster.
pub trait TableDirResolver {
    fn table_dir(&self, table: &TableKey) -> std::path::PathBuf;
}

/// Hook invoked after a successful quarantine to refill the lost rows from a
/// healthy replica. The repair (converge) action is a follow-up; for this
/// slice the hook is a logged no-op extension point so the wiring exists and
/// the contract is visible.
pub trait RefillScheduler {
    /// Schedule a refill of `table`'s affected ranges from a healthy replica.
    fn schedule_refill(&self, table: &TableKey, generations: &[u64]);
}

/// A refill scheduler that only logs — the placeholder until the converge
/// (anti-entropy repair) follow-up lands.
pub struct LoggingRefillScheduler;

impl RefillScheduler for LoggingRefillScheduler {
    fn schedule_refill(&self, table: &TableKey, generations: &[u64]) {
        tracing::info!(
            keyspace = %table.keyspace,
            table = %table.table,
            generations = ?generations,
            "self-heal: scheduled refill-from-replica for quarantined generations \
             (converge action is a follow-up; recorded as pending)"
        );
    }
}

/// Execute one decided action against the engine's on-disk state.
///
/// `posture` is the *current* replica posture for the table — re-verified here
/// as the FMEA #1 last line of defence. `dirs` resolves the table directory;
/// `refill` schedules the post-quarantine repair.
pub fn execute_action(
    action: Action,
    posture: ReplicaPosture,
    dirs: &dyn TableDirResolver,
    refill: &dyn RefillScheduler,
) -> ActionOutcome {
    match action {
        Action::QuarantineCorrupt { table, generations } => {
            // FMEA #1 defence in depth: never move files unless a healthy
            // replica can refill. If the snapshot said "quarantine" but the
            // live posture disagrees, refuse loudly — never lose data.
            if !posture.can_refill() {
                metrics::inc_quarantine_refused_no_replica();
                metrics::set_degraded(true);
                tracing::error!(
                    keyspace = %table.keyspace,
                    table = %table.table,
                    posture = ?posture,
                    "self-heal: REFUSING to quarantine corrupt generations — no healthy \
                     replica to refill; files left in place. Health=degraded. (FMEA #1)"
                );
                return ActionOutcome::Escalated {
                    table,
                    reason: "no healthy replica to refill quarantined data".to_string(),
                };
            }

            let table_dir = dirs.table_dir(&table);
            let mut moved = Vec::new();
            for &gen in &generations {
                match StorageEngine::quarantine_corrupt_generation(&table_dir, gen) {
                    Ok(quarantine_dir) => {
                        tracing::info!(
                            keyspace = %table.keyspace,
                            table = %table.table,
                            generation = gen,
                            quarantine_dir = %quarantine_dir.display(),
                            "self-heal: quarantined corrupt generation (files moved, not deleted)"
                        );
                        metrics::inc_quarantined(1);
                        moved.push(gen);
                    }
                    Err(e) => {
                        tracing::error!(
                            keyspace = %table.keyspace,
                            table = %table.table,
                            generation = gen,
                            %e,
                            "self-heal: failed to quarantine corrupt generation; left in place"
                        );
                    }
                }
            }

            if moved.is_empty() {
                metrics::set_degraded(true);
                return ActionOutcome::Escalated {
                    table,
                    reason: "quarantine moved no files (all moves failed)".to_string(),
                };
            }

            metrics::inc_actions_executed();
            refill.schedule_refill(&table, &moved);
            ActionOutcome::Quarantined {
                table,
                generations: moved,
            }
        }
        Action::Escalate { table, reason, .. } => {
            metrics::set_degraded(true);
            if matches!(reason, EscalateReason::MaxAttemptsExhausted) {
                metrics::inc_escalated_max_attempts();
            } else {
                metrics::inc_quarantine_refused_no_replica();
            }
            let reason_str = match reason {
                EscalateReason::NoHealthyReplica => {
                    "no healthy replica to refill — quarantine refused"
                }
                EscalateReason::MaxAttemptsExhausted => {
                    "remediation exhausted max attempts without resolving"
                }
            };
            tracing::error!(
                keyspace = %table.keyspace,
                table = %table.table,
                reason = reason_str,
                "self-heal: ESCALATING issue to degraded; controller will stop retrying"
            );
            ActionOutcome::Escalated {
                table,
                reason: reason_str.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_heal::test_fixtures::{corrupt_one_generation, table_dir_with_n_generations};
    use serial_test::serial;
    use std::path::PathBuf;

    struct FixedDir(PathBuf);
    impl TableDirResolver for FixedDir {
        fn table_dir(&self, _t: &TableKey) -> PathBuf {
            self.0.clone()
        }
    }

    struct RecordingRefill {
        called: std::sync::atomic::AtomicBool,
    }
    impl RefillScheduler for RecordingRefill {
        fn schedule_refill(&self, _t: &TableKey, _g: &[u64]) {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    #[serial]
    fn quarantine_with_healthy_replica_moves_files_and_schedules_refill() {
        metrics::_reset_self_heal_metrics_for_tests();
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let gen = corrupt_one_generation(&table_dir);

        let dirs = FixedDir(table_dir.clone());
        let refill = RecordingRefill {
            called: std::sync::atomic::AtomicBool::new(false),
        };
        let action = Action::QuarantineCorrupt {
            table: TableKey::new("test_ks", "test_table"),
            generations: vec![gen],
        };
        let outcome = execute_action(
            action,
            ReplicaPosture::HealthyReplicaAvailable,
            &dirs,
            &refill,
        );

        match outcome {
            ActionOutcome::Quarantined { generations, .. } => {
                assert_eq!(generations, vec![gen]);
            }
            other => panic!("expected quarantine, got {other:?}"),
        }
        // Files MOVED, not deleted: quarantine dir holds the gen's Data.db.
        let quarantine_dir = table_dir.join("quarantine");
        assert!(quarantine_dir.exists());
        let names: Vec<String> = std::fs::read_dir(&quarantine_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            names.iter().any(|n| n.ends_with("-Data.db")),
            "corrupt gen files must be preserved in quarantine, got {names:?}"
        );
        assert!(
            refill.called.load(std::sync::atomic::Ordering::SeqCst),
            "refill from replica must be scheduled"
        );
        assert_eq!(
            metrics::self_heal_metrics().quarantined_generations_total,
            1
        );
        assert!(!metrics::self_heal_metrics().degraded);
    }

    #[test]
    #[serial]
    fn quarantine_refused_when_no_healthy_replica_leaves_files_and_degrades() {
        metrics::_reset_self_heal_metrics_for_tests();
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let gen = corrupt_one_generation(&table_dir);

        let dirs = FixedDir(table_dir.clone());
        let refill = RecordingRefill {
            called: std::sync::atomic::AtomicBool::new(false),
        };
        let action = Action::QuarantineCorrupt {
            table: TableKey::new("test_ks", "test_table"),
            generations: vec![gen],
        };
        // Live posture says no healthy replica → executor must REFUSE even
        // though it was handed a quarantine action (defence in depth).
        let outcome = execute_action(action, ReplicaPosture::SingleNode, &dirs, &refill);

        assert!(matches!(outcome, ActionOutcome::Escalated { .. }));
        assert!(
            !table_dir.join("quarantine").exists(),
            "no files may be moved when refusing for safety"
        );
        // Corrupt gen's Data.db is still in place for salvage.
        assert!(
            StorageEngine::generation_component_path_for_test(&table_dir, gen, "Data.db").is_some()
        );
        assert!(!refill.called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            metrics::self_heal_metrics().quarantined_generations_total,
            0
        );
        assert_eq!(
            metrics::self_heal_metrics().quarantine_refused_no_replica_total,
            1
        );
        assert!(metrics::self_heal_metrics().degraded);
    }
}
