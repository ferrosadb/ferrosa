//! Deterministic, autonomous self-healing controller.
//!
//! See `specs/proposed/self-healing-controller-design.md` and its FMEA. This
//! module implements the **core control loop + the quarantine action** —
//! the first vertical slice. Drain (compaction), converge (anti-entropy
//! repair) and divergence detection are explicit follow-ups; the extension
//! points are marked throughout.
//!
//! ## Shape
//!
//! - [`config::SelfHealConfig`] — fixed, env-or-default knobs.
//! - [`snapshot::HealthSnapshot`] — plain-data observable state (issues +
//!   attempt ledger + logical tick + ring). The decision input.
//! - [`decide::decide`] — the **pure** decision function. No clock/RNG/IO.
//! - [`detector`] — corrupt-SSTable detector (loud WARN + metric, always).
//! - [`action`] — quarantine executor with the FMEA #1 safety rail.
//! - [`metrics`] — counters/gauges + the health surface.
//! - [`SelfHealController`] — owns the ledger, builds snapshots each tick,
//!   calls `decide`, executes one action, and re-publishes health.
//!
//! ## Determinism
//!
//! The loop's only nondeterministic input is *when* a tick fires (wall-clock
//! sleep). Everything the decision depends on is the [`HealthSnapshot`], built
//! from observable state, advanced by a **logical** tick counter — so a
//! recorded snapshot sequence reproduces the exact action sequence.

pub mod action;
pub mod config;
pub mod decide;
pub mod detector;
pub mod metrics;
pub mod snapshot;

#[cfg(any(test, feature = "test-support"))]
pub mod test_fixtures;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::engine::StorageEngine;
use crate::TableId;

pub use config::SelfHealConfig;
pub use decide::{Action, EscalateReason};
pub use metrics::{self_heal_health, self_heal_metrics, SelfHealHealth, SelfHealMetrics};
pub use snapshot::{
    AttemptState, CorruptSstable, HealthSnapshot, IssueKind, ReplicaPosture, RingView, TableIssue,
    TableKey,
};

use action::{ActionOutcome, LoggingRefillScheduler, RefillScheduler, TableDirResolver};
use metrics::{HealthEntry, SelfHealHealth as Health};

/// Supplies the cluster-level facts the controller cannot read from the
/// storage engine directly (the storage crate sits below the cluster crate).
///
/// **This is the integration seam the lead asked about.** A follow-up wires a
/// `ferrosa-cluster` implementation that answers replica health from the ring
/// and peer reachability. Until then [`SingleNodeClusterView`] is used, which
/// reports [`ReplicaPosture::SingleNode`] (the safe, never-quarantine posture;
/// FMEA #1: a single-node engine must never quarantine).
pub trait ClusterView: Send + Sync {
    /// This node's host-id (for deterministic initiator selection).
    fn this_host(&self) -> u64;

    /// Owners (host-ids) of the table's ranges, or `None` if unknown / single
    /// node. Used to pick the deterministic initiator (lowest host-id).
    fn owners(&self, table: &TableKey) -> Option<Vec<u64>>;

    /// Whether a healthy peer replica can refill the table's affected range
    /// (FMEA #1 gate). The default safe answer is [`ReplicaPosture::SingleNode`].
    fn replica_posture(&self, table: &TableKey) -> ReplicaPosture;
}

/// A [`ClusterView`] for single-node deployments: this host owns everything
/// and no peer can refill, so corruption escalates rather than quarantines.
#[derive(Default)]
pub struct SingleNodeClusterView {
    pub host_id: u64,
}

impl ClusterView for SingleNodeClusterView {
    fn this_host(&self) -> u64 {
        self.host_id
    }
    fn owners(&self, _table: &TableKey) -> Option<Vec<u64>> {
        None
    }
    fn replica_posture(&self, _table: &TableKey) -> ReplicaPosture {
        ReplicaPosture::SingleNode
    }
}

/// Resolves a table's on-disk SSTable directory via the engine.
struct EngineDirResolver {
    engine: Arc<StorageEngine>,
}

impl TableDirResolver for EngineDirResolver {
    fn table_dir(&self, table: &TableKey) -> std::path::PathBuf {
        self.engine
            .table_sstable_dir(&TableId::new(&table.keyspace, &table.table))
    }
}

/// The control loop. Owns the cross-tick attempt ledger and orchestrates
/// detect → decide → act → verify → publish each tick.
pub struct SelfHealController {
    engine: Arc<StorageEngine>,
    cluster: Arc<dyn ClusterView>,
    config: SelfHealConfig,
    /// Per-(table, issue) attempt ledger carried across ticks (the mutable
    /// state `decide` reads but never mutates).
    ledger: BTreeMap<(TableKey, IssueKind), AttemptState>,
    /// Logical tick counter — the snapshot's deterministic clock.
    tick: u64,
    refill: Box<dyn RefillScheduler + Send + Sync>,
}

impl SelfHealController {
    /// Construct a controller. `cluster` supplies replica-health facts.
    pub fn new(
        engine: Arc<StorageEngine>,
        cluster: Arc<dyn ClusterView>,
        config: SelfHealConfig,
    ) -> Self {
        Self {
            engine,
            cluster,
            config,
            ledger: BTreeMap::new(),
            tick: 0,
            refill: Box::new(LoggingRefillScheduler),
        }
    }

    /// Spawn the controller on its own tokio task, gated by the master switch.
    /// Returns the join handle (or `None` if disabled at spawn). Cheap when
    /// idle: it sleeps `tick_interval` between passes and only scans
    /// registered tables.
    pub fn spawn(
        engine: Arc<StorageEngine>,
        cluster: Arc<dyn ClusterView>,
        config: SelfHealConfig,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !config.enabled {
            tracing::info!(
                "self-heal: controller disabled via {} — detection/remediation off",
                config::ENV_ENABLED
            );
            // Disabled: we do not even spawn. (Detection is part of the loop;
            // an operator who disables the controller opts out of autonomous
            // scanning. Startup smoke-test warnings still fire independently.)
            return None;
        }
        let interval = config.tick_interval;
        let mut controller = SelfHealController::new(engine, cluster, config);
        tracing::info!(
            tick_secs = interval.as_secs(),
            "self-heal: controller starting"
        );
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                controller.run_one_tick();
            }
        }))
    }

    /// List the tables the controller scans. Currently every registered table.
    fn scan_targets(&self) -> Vec<(TableKey, std::path::PathBuf)> {
        self.engine
            .registered_table_ids()
            .into_iter()
            .map(|tid| {
                let dir = self.engine.table_sstable_dir(&tid);
                (TableKey::new(tid.keyspace(), tid.table()), dir)
            })
            .collect()
    }

    /// Build the snapshot for the current tick from observable state.
    fn build_snapshot(&self) -> HealthSnapshot {
        let mut issues = Vec::new();
        let mut owners_by_table = BTreeMap::new();

        for (table, dir) in self.scan_targets() {
            if let Some(owners) = self.cluster.owners(&table) {
                owners_by_table.insert(table.clone(), owners);
            }
            let posture = self.cluster.replica_posture(&table);
            // Detector emits the loud WARN + metric unconditionally.
            if let Some(issue) = detector::detect_corrupt_sstables(&table, &dir, posture) {
                issues.push(issue);
            }
            // Extension point: detector::detect_bloat / detect_divergence here.
        }

        HealthSnapshot {
            tick: self.tick,
            issues,
            ledger: self.ledger.clone(),
            ring: RingView {
                this_host: self.cluster.this_host(),
                owners_by_table,
            },
        }
    }

    /// One full control-loop pass. Public for deterministic step-testing
    /// without a tokio runtime.
    pub fn run_one_tick(&mut self) {
        let snapshot = self.build_snapshot();

        // Update the gauge of currently-corrupt tables (loud-on-issue surface).
        let corrupt_tables = snapshot
            .issues
            .iter()
            .filter(|i| i.kind == IssueKind::CorruptSstables)
            .count() as u64;
        metrics::set_corrupt_tables(corrupt_tables);

        let decision = decide::decide(&snapshot, &self.config);
        let outcome = self.apply_decision(&snapshot, decision);

        self.publish_health(&snapshot, outcome.as_ref());
        self.tick = self.tick.saturating_add(1);
    }

    /// Execute the chosen action (if any) and update the ledger.
    fn apply_decision(
        &mut self,
        snapshot: &HealthSnapshot,
        decision: Option<Action>,
    ) -> Option<ActionOutcome> {
        let action = decision?;
        // Identify the (table, issue) this action targets so we can record an
        // attempt against the correct ledger entry.
        let (table, kind) = match &action {
            Action::QuarantineCorrupt { table, .. } => (table.clone(), IssueKind::CorruptSstables),
            Action::Escalate { table, kind, .. } => (table.clone(), *kind),
        };

        let posture = self.cluster.replica_posture(&table);
        let dirs = EngineDirResolver {
            engine: self.engine.clone(),
        };
        let outcome = action::execute_action(action, posture, &dirs, self.refill.as_ref());

        // Ledger update — deterministic bookkeeping (FMEA #5 escalate-and-stop).
        let entry = self.ledger.entry((table, kind)).or_default();
        match &outcome {
            ActionOutcome::Quarantined { .. } => {
                entry.attempts = entry.attempts.saturating_add(1);
                entry.last_attempt_tick = Some(snapshot.tick);
                // Not escalated: next tick re-runs the detector to verify the
                // issue cleared (verify-by-re-detection, FMEA #11).
            }
            ActionOutcome::Escalated { .. } => {
                entry.attempts = entry.attempts.saturating_add(1);
                entry.last_attempt_tick = Some(snapshot.tick);
                entry.escalated = true;
            }
        }
        Some(outcome)
    }

    /// Publish the health surface for this tick.
    fn publish_health(&self, snapshot: &HealthSnapshot, outcome: Option<&ActionOutcome>) {
        let mut entries = Vec::new();
        let mut degraded = false;

        for issue in &snapshot.issues {
            let state = self
                .ledger
                .get(&issue.ledger_key())
                .copied()
                .unwrap_or_default();
            let status = if state.escalated {
                degraded = true;
                "escalated (degraded)".to_string()
            } else if matches!(outcome, Some(ActionOutcome::Quarantined { table, .. }) if *table == issue.table)
            {
                "quarantined this tick".to_string()
            } else {
                format!("detected ({} attempt(s))", state.attempts)
            };
            entries.push(HealthEntry {
                table: issue.table.to_string(),
                issue: issue.kind.as_str().to_string(),
                status,
            });
        }

        metrics::publish_health(Health {
            entries,
            degraded,
            last_tick: snapshot.tick,
        });
    }

    /// Test accessor: current ledger.
    #[cfg(test)]
    pub(crate) fn ledger(&self) -> &BTreeMap<(TableKey, IssueKind), AttemptState> {
        &self.ledger
    }

    /// Test accessor: current logical tick.
    #[cfg(test)]
    pub(crate) fn tick(&self) -> u64 {
        self.tick
    }
}

#[cfg(test)]
mod controller_tests {
    use super::*;
    use serial_test::serial;
    use test_fixtures::{corrupt_one_generation, table_dir_with_n_generations_engine};

    /// A cluster view we can pin per test.
    struct PinnedCluster {
        host: u64,
        posture: ReplicaPosture,
        owners: Option<Vec<u64>>,
    }
    impl ClusterView for PinnedCluster {
        fn this_host(&self) -> u64 {
            self.host
        }
        fn owners(&self, _t: &TableKey) -> Option<Vec<u64>> {
            self.owners.clone()
        }
        fn replica_posture(&self, _t: &TableKey) -> ReplicaPosture {
            self.posture
        }
    }

    #[test]
    #[serial]
    fn tick_with_no_corruption_is_idle() {
        metrics::_reset_self_heal_metrics_for_tests();
        let engine = table_dir_with_n_generations_engine(2);
        let cluster = Arc::new(PinnedCluster {
            host: 0,
            posture: ReplicaPosture::HealthyReplicaAvailable,
            owners: None,
        });
        let mut c = SelfHealController::new(engine, cluster, SelfHealConfig::default());
        c.run_one_tick();
        assert_eq!(metrics::self_heal_metrics().corrupt_sstable_tables, 0);
        assert_eq!(metrics::self_heal_metrics().actions_executed_total, 0);
        assert_eq!(c.tick(), 1);
    }

    #[test]
    #[serial]
    fn single_node_corruption_escalates_never_quarantines() {
        metrics::_reset_self_heal_metrics_for_tests();
        let engine = table_dir_with_n_generations_engine(2);
        let table = TableKey::new("test_ks", "test_table");
        corrupt_one_generation(&engine.table_sstable_dir(&TableId::new("test_ks", "test_table")));

        let cluster = Arc::new(PinnedCluster {
            host: 0,
            posture: ReplicaPosture::SingleNode,
            owners: None,
        });
        let mut c = SelfHealController::new(engine.clone(), cluster, SelfHealConfig::default());
        c.run_one_tick();

        // No files moved; degraded; escalated in ledger.
        let table_dir = engine.table_sstable_dir(&TableId::new("test_ks", "test_table"));
        assert!(
            !table_dir.join("quarantine").exists(),
            "single-node corruption must NOT quarantine"
        );
        let m = metrics::self_heal_metrics();
        assert_eq!(m.quarantined_generations_total, 0);
        assert!(m.degraded, "must mark degraded");
        let state = c
            .ledger()
            .get(&(table.clone(), IssueKind::CorruptSstables))
            .copied()
            .unwrap_or_default();
        assert!(state.escalated, "issue must be escalated-and-stopped");

        // Second tick: escalated issue is inert (no thrash).
        let before = metrics::self_heal_metrics().quarantine_refused_no_replica_total;
        c.run_one_tick();
        let after = metrics::self_heal_metrics().quarantine_refused_no_replica_total;
        assert_eq!(before, after, "escalated issue must not loop / re-act");
    }

    #[test]
    #[serial]
    fn healthy_replica_corruption_quarantines_and_clears() {
        metrics::_reset_self_heal_metrics_for_tests();
        let engine = table_dir_with_n_generations_engine(2);
        let table_dir = engine.table_sstable_dir(&TableId::new("test_ks", "test_table"));
        let gen = corrupt_one_generation(&table_dir);

        let cluster = Arc::new(PinnedCluster {
            host: 0,
            posture: ReplicaPosture::HealthyReplicaAvailable,
            owners: None,
        });
        let mut c = SelfHealController::new(engine, cluster, SelfHealConfig::default());
        c.run_one_tick();

        // Gen moved to quarantine; healthy gen still on disk.
        let quarantine_dir = table_dir.join("quarantine");
        assert!(quarantine_dir.exists());
        let moved: Vec<String> = std::fs::read_dir(&quarantine_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(moved.iter().any(|n| n.starts_with(&format!("{gen}-"))));
        let m = metrics::self_heal_metrics();
        assert_eq!(m.quarantined_generations_total, 1);
        assert_eq!(m.actions_executed_total, 1);

        // Next tick: corruption is gone (gen removed), so no further action.
        let before = metrics::self_heal_metrics().quarantined_generations_total;
        c.run_one_tick();
        assert_eq!(
            metrics::self_heal_metrics().quarantined_generations_total,
            before,
            "issue resolved → no further quarantine"
        );
    }

    #[test]
    #[serial]
    fn non_initiator_node_does_not_quarantine() {
        metrics::_reset_self_heal_metrics_for_tests();
        let engine = table_dir_with_n_generations_engine(2);
        let table_dir = engine.table_sstable_dir(&TableId::new("test_ks", "test_table"));
        corrupt_one_generation(&table_dir);

        // This host is 9, but owners include 1 → initiator is 1, not us.
        let cluster = Arc::new(PinnedCluster {
            host: 9,
            posture: ReplicaPosture::HealthyReplicaAvailable,
            owners: Some(vec![1, 9]),
        });
        let mut c = SelfHealController::new(engine, cluster, SelfHealConfig::default());
        c.run_one_tick();
        assert!(
            !table_dir.join("quarantine").exists(),
            "non-initiator must defer to the deterministic initiator (FMEA #4)"
        );
        assert_eq!(
            metrics::self_heal_metrics().quarantined_generations_total,
            0
        );
    }
}
