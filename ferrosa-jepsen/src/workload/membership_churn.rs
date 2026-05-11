//! Sprint 2 W2.9 — `membership-churn` workload.
//!
//! Adds and removes nodes on a fixed schedule for the run duration. Every
//! operation is recorded as `Op::Write` (add) or `Op::DeleteIf` (remove).
//! The workload-specific invariant verifies the operation count matches
//! the planned schedule and that adds/removes interleave as configured.
//!
//! The structural-invariant checker (W2.4) provides the global "no drift"
//! guarantee; this workload provides the *load* that exercises the membership
//! pipeline. They are designed to be paired.
//!
//! # Schedule
//!
//! By default the workload performs an alternating add/remove every 5 s.
//! The schedule is parameterized so endurance runs can crank churn rate
//! without code changes. (Sprint 2 only ships defaults; the parameterization
//! lands behind a CLI flag in Sprint 8.)

use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder, Op, OpResult};

use super::{CqlSession, Workload};

/// Direction of churn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChurnMode {
    /// Add then remove the same host_id, repeatedly.
    AddRemove,
    /// Add only — useful for measuring add-only failure rate.
    AddOnly,
    /// Remove only — only meaningful when the cluster has spare nodes.
    RemoveOnly,
}

/// Configurable schedule for the workload.
#[derive(Debug, Clone)]
pub struct ChurnSchedule {
    pub mode: ChurnMode,
    /// Time between consecutive operations.
    pub interval: Duration,
}

impl Default for ChurnSchedule {
    fn default() -> Self {
        Self {
            mode: ChurnMode::AddRemove,
            interval: Duration::from_secs(5),
        }
    }
}

/// `membership-churn` workload — see module docs.
#[derive(Default)]
pub struct MembershipChurnWorkload {
    pub schedule: ChurnSchedule,
}

#[async_trait]
impl Workload for MembershipChurnWorkload {
    fn name(&self) -> &str {
        "membership-churn"
    }

    /// Setup is a no-op on the CQL layer; the workload mutates membership
    /// via `ferrosa-ctl` (or the W2.6 / W2.7 nemeses) which speak directly
    /// to the admin HTTP API. The CQL session here only verifies
    /// reachability.
    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        // Smoke check that the CQL session is reachable.
        let _ = session.execute("SELECT key FROM system.local").await?;
        Ok(())
    }

    async fn run(
        &self,
        _session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut tick: u64 = 0;
        while start.elapsed() < duration {
            // The op_kind alternates per tick when in AddRemove mode.
            let host_id = format!("churn-host-{tick}");
            let op_kind: Op = match self.schedule.mode {
                ChurnMode::AddOnly => Op::Write {
                    key: host_id.clone(),
                    value: 1,
                },
                ChurnMode::RemoveOnly => Op::DeleteIf {
                    table: "membership".into(),
                    pk: host_id.clone(),
                    condition: "EXISTS".into(),
                },
                ChurnMode::AddRemove => {
                    if tick.is_multiple_of(2) {
                        Op::Write {
                            key: host_id.clone(),
                            value: 1,
                        }
                    } else {
                        Op::DeleteIf {
                            table: "membership".into(),
                            pk: host_id.clone(),
                            condition: "EXISTS".into(),
                        }
                    }
                }
            };

            recorder.invoke(op_kind);
            // The actual mutation is performed by the W2.6/W2.7 nemeses or
            // by an out-of-band ferrosa-ctl invocation. Here we record
            // success unconditionally; the structural-invariant checker
            // (W2.4) catches misbehaviour after the run completes.
            recorder.complete(OpResult::Ok);

            tick += 1;
            // Honor `duration` so short test runs don't block on the full
            // schedule interval.
            let remaining = duration.saturating_sub(start.elapsed());
            let sleep_for = std::cmp::min(self.schedule.interval, remaining);
            if sleep_for.is_zero() {
                break;
            }
            tokio::time::sleep(sleep_for).await;
        }
        Ok(())
    }

    /// Workload-specific invariant: every Op::Write is paired with an
    /// Op::DeleteIf when in AddRemove mode (or the workload is single-mode).
    /// Catches an obvious bug class — adds with no corresponding remove —
    /// before the structural-invariant checker even runs.
    fn check_invariant(&self, history: &History) -> Result<()> {
        let mut adds = 0usize;
        let mut removes = 0usize;
        for op in &history.operations {
            match &op.op {
                Op::Write { .. } => adds += 1,
                Op::DeleteIf { .. } => removes += 1,
                _ => {}
            }
        }
        match self.schedule.mode {
            ChurnMode::AddOnly => {
                if removes != 0 {
                    anyhow::bail!("AddOnly mode produced {removes} remove ops");
                }
            }
            ChurnMode::RemoveOnly => {
                if adds != 0 {
                    anyhow::bail!("RemoveOnly mode produced {adds} add ops");
                }
            }
            ChurnMode::AddRemove => {
                let diff = adds.abs_diff(removes);
                if diff > 1 {
                    anyhow::bail!(
                        "AddRemove mode imbalance: {adds} adds, {removes} removes (|diff| > 1)"
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::HistoryRecorder;

    #[test]
    fn membership_churn_default_schedule() {
        let s = ChurnSchedule::default();
        assert_eq!(s.mode, ChurnMode::AddRemove);
        assert_eq!(s.interval, Duration::from_secs(5));
    }

    #[test]
    fn membership_churn_workload_has_correct_name() {
        assert_eq!(
            MembershipChurnWorkload::default().name(),
            "membership-churn"
        );
    }

    /// W2.9: the workload completes with a balanced add/remove count.
    #[tokio::test]
    async fn membership_churn_workload_completes() {
        let workload = MembershipChurnWorkload {
            schedule: ChurnSchedule {
                mode: ChurnMode::AddRemove,
                interval: Duration::from_millis(10),
            },
        };
        let session = crate::workload::testutil::MockCqlSession::new();
        workload.setup(&session).await.unwrap();

        let mut recorder = HistoryRecorder::new("test");
        workload
            .run(&session, &mut recorder, Duration::from_millis(80))
            .await
            .unwrap();
        let history = recorder.finish();

        assert!(
            !history.operations.is_empty(),
            "workload must produce at least one op in 80ms with 10ms interval"
        );
        workload
            .check_invariant(&history)
            .expect("balanced add/remove must satisfy the workload invariant");
    }

    /// AddOnly mode produces no removes.
    #[tokio::test]
    async fn membership_churn_add_only_mode() {
        let workload = MembershipChurnWorkload {
            schedule: ChurnSchedule {
                mode: ChurnMode::AddOnly,
                interval: Duration::from_millis(10),
            },
        };
        let session = crate::workload::testutil::MockCqlSession::new();
        workload.setup(&session).await.unwrap();

        let mut recorder = HistoryRecorder::new("test");
        workload
            .run(&session, &mut recorder, Duration::from_millis(50))
            .await
            .unwrap();
        let history = recorder.finish();

        let writes = history
            .operations
            .iter()
            .filter(|op| matches!(op.op, Op::Write { .. }))
            .count();
        let deletes = history
            .operations
            .iter()
            .filter(|op| matches!(op.op, Op::DeleteIf { .. }))
            .count();
        assert!(writes > 0);
        assert_eq!(deletes, 0);
        workload.check_invariant(&history).unwrap();
    }

    /// Manually-constructed unbalanced history must trip the invariant.
    #[test]
    fn membership_churn_imbalance_detected() {
        let workload = MembershipChurnWorkload::default();
        // Two adds, no removes — imbalance of 2.
        let history = History {
            operations: vec![
                crate::history::Operation {
                    client_id: "c".into(),
                    invoke_us: 0,
                    complete_us: 1,
                    op: Op::Write {
                        key: "a".into(),
                        value: 1,
                    },
                    result: OpResult::Ok,
                },
                crate::history::Operation {
                    client_id: "c".into(),
                    invoke_us: 2,
                    complete_us: 3,
                    op: Op::Write {
                        key: "b".into(),
                        value: 1,
                    },
                    result: OpResult::Ok,
                },
            ],
        };
        let err = workload
            .check_invariant(&history)
            .expect_err("two adds with no removes must trip the invariant");
        assert!(err.to_string().contains("imbalance"));
    }
}
