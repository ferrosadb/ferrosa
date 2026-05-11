//! Sprint 2 W2.11 — `late-join-flood` workload.
//!
//! Burst-adds N (default: 5) fresh nodes simultaneously and waits for them
//! to converge. The post-run structural-invariant check (W2.4) verifies
//! every late joiner appears in every existing node's snapshot.
//!
//! The workload itself records each "join attempt" as an `Op::Write` so
//! the orchestrator's history accounting picks it up. The actual node
//! spawning is performed by the W2.6 add-node-via-follower nemesis,
//! parameterized to fire N times in parallel.

use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder, Op, OpResult};

use super::{CqlSession, Workload};

/// Number of late joiners spawned per burst.
pub const DEFAULT_BURST_SIZE: usize = 5;

/// `late-join-flood` workload — see module docs.
pub struct LateJoinFloodWorkload {
    pub burst_size: usize,
    /// Time to wait between successive bursts. The default is 30s; tests
    /// override this for fast iteration.
    pub burst_interval: Duration,
}

impl Default for LateJoinFloodWorkload {
    fn default() -> Self {
        Self {
            burst_size: DEFAULT_BURST_SIZE,
            burst_interval: Duration::from_secs(30),
        }
    }
}

#[async_trait]
impl Workload for LateJoinFloodWorkload {
    fn name(&self) -> &str {
        "late-join-flood"
    }

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
        let mut burst_idx: u64 = 0;

        while start.elapsed() < duration {
            // Record each join attempt in the burst as a Write op so the
            // orchestrator surfaces operation count.
            for joiner in 0..self.burst_size {
                let host = format!("late-burst{burst_idx}-host{joiner}");
                recorder.invoke(Op::Write {
                    key: host,
                    value: 1,
                });
                recorder.complete(OpResult::Ok);
            }

            burst_idx += 1;
            // Sleep until the next burst, but honor `duration` so a short
            // run loop doesn't block on the full burst_interval.
            let remaining = duration.saturating_sub(start.elapsed());
            let sleep_for = std::cmp::min(self.burst_interval, remaining);
            if sleep_for.is_zero() {
                break;
            }
            tokio::time::sleep(sleep_for).await;
        }
        Ok(())
    }

    /// Workload-specific invariant: every joiner recorded in the history
    /// has a matching write op (we don't have read-back semantics here;
    /// the structural-invariant checker verifies cluster-wide convergence).
    /// The check ensures we did at least one burst — i.e. the workload
    /// actually ran.
    fn check_invariant(&self, history: &History) -> Result<()> {
        if history.operations.is_empty() {
            anyhow::bail!("late-join-flood produced no operations; workload did not run");
        }
        let writes = history
            .operations
            .iter()
            .filter(|op| matches!(op.op, Op::Write { .. }))
            .count();
        if writes == 0 {
            anyhow::bail!("late-join-flood produced no Write ops; bug in run()");
        }
        // Check at least one full burst happened.
        if writes < self.burst_size {
            anyhow::bail!(
                "late-join-flood: only {writes} writes recorded but burst_size={} \
                 — at least one full burst must complete",
                self.burst_size
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::HistoryRecorder;

    #[test]
    fn late_join_flood_workload_has_correct_name() {
        assert_eq!(LateJoinFloodWorkload::default().name(), "late-join-flood");
    }

    #[test]
    fn late_join_flood_default_burst_size() {
        assert_eq!(LateJoinFloodWorkload::default().burst_size, 5);
    }

    /// W2.11: a single burst converges to burst_size writes recorded.
    #[tokio::test]
    async fn late_join_flood_converges() {
        let workload = LateJoinFloodWorkload {
            burst_size: 5,
            burst_interval: Duration::from_millis(10),
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
        assert!(
            writes >= 5,
            "at least one full burst (5 writes) must complete; got {writes}"
        );
        workload
            .check_invariant(&history)
            .expect("converged history must satisfy the workload invariant");
    }

    /// Empty history fails the invariant.
    #[test]
    fn late_join_flood_invariant_rejects_empty_history() {
        let workload = LateJoinFloodWorkload::default();
        let history = History { operations: vec![] };
        let err = workload
            .check_invariant(&history)
            .expect_err("empty history must fail");
        assert!(err.to_string().contains("did not run"));
    }

    /// Partial-burst history fails the invariant.
    #[test]
    fn late_join_flood_invariant_rejects_partial_burst() {
        let workload = LateJoinFloodWorkload::default();
        let history = History {
            operations: vec![crate::history::Operation {
                client_id: "c".into(),
                invoke_us: 0,
                complete_us: 1,
                op: Op::Write {
                    key: "h".into(),
                    value: 1,
                },
                result: OpResult::Ok,
            }],
        };
        let err = workload
            .check_invariant(&history)
            .expect_err("a single write is shorter than the burst size");
        assert!(err.to_string().contains("burst_size=5"));
    }
}
