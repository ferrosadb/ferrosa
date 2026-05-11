//! Sprint 2 W2.10 — `forward-probe` workload.
//!
//! Specifically targets the bug class fixed in Sprint 1 W1.5: a non-leader
//! that silently drops a membership-mutating proposal instead of forwarding
//! it to the leader. The pre-fix symptom is that `UPDATE … FROM jepsen.peers`
//! issued against a follower returns OK locally but the apply never reaches
//! the leader, so the change is silently lost.
//!
//! This workload:
//!
//! 1. Creates `jepsen.peers (host_id text PRIMARY KEY, metadata text)` and
//!    seeds three rows.
//! 2. Repeatedly issues UPDATE statements that bump `metadata` to a fresh
//!    monotonic value, then SELECTs to verify the update is visible.
//! 3. Records each UPDATE/SELECT pair as an `Op::Write` / `Op::Read` so the
//!    invariant check (`check_invariant`) can verify monotonicity:
//!    every successfully-applied UPDATE for a given host_id must be
//!    visible to the next SELECT.
//!
//! The orchestrator pairs this workload with the `partition-halves` or
//! `kill-minority` nemesis to force at least one operation through a
//! follower; the silent-drop bug class always violated the read-after-write
//! invariant when that path was exercised.

use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder, Op, OpResult, Operation};

use super::{CqlSession, Workload};

/// Forward-probe workload — see module docs.
pub struct ForwardProbeWorkload;

#[async_trait]
impl Workload for ForwardProbeWorkload {
    fn name(&self) -> &str {
        "forward-probe"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session
            .execute(
                "CREATE KEYSPACE IF NOT EXISTS jepsen \
                 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
            )
            .await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.peers \
                 (host_id text PRIMARY KEY, metadata int)",
            )
            .await?;
        // Seed the three rows the workload mutates.
        for host_id in ["0", "1", "2"] {
            session
                .execute(&format!(
                    "INSERT INTO jepsen.peers (host_id, metadata) VALUES ('{host_id}', 0)"
                ))
                .await?;
        }
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter: i64 = 1;
        while start.elapsed() < duration {
            let host = format!("{}", counter % 3);

            // 1. UPDATE
            recorder.invoke(Op::Write {
                key: host.clone(),
                value: counter,
            });
            let upd = session
                .execute(&format!(
                    "UPDATE jepsen.peers SET metadata = {counter} \
                     WHERE host_id = '{host}'"
                ))
                .await;
            match upd {
                Ok(_) => recorder.complete(OpResult::Ok),
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }

            // 2. SELECT to verify the UPDATE took effect.
            recorder.invoke(Op::Read { key: host.clone() });
            let sel = session
                .execute(&format!(
                    "SELECT metadata FROM jepsen.peers WHERE host_id = '{host}'"
                ))
                .await;
            match sel {
                Ok(rows) => {
                    let val = rows
                        .first()
                        .and_then(|r| r.first())
                        .and_then(|(_, v)| v.parse::<i64>().ok());
                    recorder.complete(OpResult::Value(val));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }

            counter += 1;
        }
        Ok(())
    }

    /// Workload-specific invariant: for each (host_id) key, every Write that
    /// completed Ok must be observable by some subsequent Read whose
    /// invoke_us > Write.complete_us. A silent-drop never produces such a
    /// Read for the dropped value.
    fn check_invariant(&self, history: &History) -> Result<()> {
        for key in unique_keys(history) {
            let ops = history.filter_key(&key).operations;
            let mut writes_completed: Vec<&Operation> = ops
                .iter()
                .filter(|op| matches!(op.op, Op::Write { .. }) && matches!(op.result, OpResult::Ok))
                .collect();
            writes_completed.sort_by_key(|o| o.complete_us);

            for w in &writes_completed {
                let value_written = match w.op {
                    Op::Write { value, .. } => value,
                    _ => unreachable!(),
                };

                // Find the FIRST read that started after the write completed.
                let first_read = ops
                    .iter()
                    .filter(|op| matches!(op.op, Op::Read { .. }) && op.invoke_us >= w.complete_us)
                    .min_by_key(|o| o.invoke_us);

                let Some(r) = first_read else { continue };
                let observed = match r.result {
                    OpResult::Value(v) => v,
                    OpResult::Timeout | OpResult::Err(_) => continue,
                    _ => continue,
                };

                if let Some(observed) = observed {
                    if observed < value_written {
                        anyhow::bail!(
                            "forward-probe invariant violated for key {key}: write({value_written}) \
                             completed at t={cw}us but the next read at t={ir}us observed {observed} \
                             (older). This is the silent-drop signature.",
                            cw = w.complete_us,
                            ir = r.invoke_us,
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

fn unique_keys(history: &History) -> Vec<String> {
    let mut s = std::collections::BTreeSet::new();
    for op in &history.operations {
        match &op.op {
            Op::Read { key } | Op::Write { key, .. } => {
                s.insert(key.clone());
            }
            _ => {}
        }
    }
    s.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{Op, OpResult, Operation};

    fn make_op(client: &str, invoke: u64, complete: u64, op: Op, result: OpResult) -> Operation {
        Operation {
            client_id: client.to_string(),
            invoke_us: invoke,
            complete_us: complete,
            op,
            result,
        }
    }

    #[test]
    fn forward_probe_workload_has_correct_name() {
        assert_eq!(ForwardProbeWorkload.name(), "forward-probe");
    }

    /// W2.10: against a CqlSession that does NOT silently drop, every write is
    /// observable by the next read. invariant returns Ok.
    #[tokio::test]
    async fn forward_probe_succeeds_against_followers() {
        // We use the testutil mock which echoes 1000 for every SELECT.
        // That means every read sees value 1000 regardless of the write,
        // so the read >= written-value guard catches the bug.
        // To test the success path, we synthesize a history directly that
        // models a healthy cluster where reads always observe the latest write.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "0".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "0".into() },
                    OpResult::Value(Some(1)),
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Write {
                        key: "0".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    700,
                    800,
                    Op::Read { key: "0".into() },
                    OpResult::Value(Some(2)),
                ),
            ],
        };
        ForwardProbeWorkload
            .check_invariant(&history)
            .expect("monotonic-history must pass the forward-probe invariant");
    }

    /// W2.10: the silent-drop signature — write(2) succeeded but the next
    /// read observed value 1 — must fail the invariant.
    #[test]
    fn forward_probe_detects_silent_drop_signature() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "0".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                // The follower silently dropped the proposal — leader sees old value.
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Write {
                        key: "0".into(),
                        value: 5,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Read { key: "0".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };
        let err = ForwardProbeWorkload
            .check_invariant(&history)
            .expect_err("silent-drop pattern must trip the invariant");
        let msg = err.to_string();
        assert!(
            msg.contains("silent-drop"),
            "error must mention the silent-drop signature; got: {msg}"
        );
    }

    /// Reads that complete with Timeout/Err must not be treated as observed
    /// stale values; the invariant tolerates indeterminate outcomes.
    #[test]
    fn forward_probe_tolerates_timeouts_and_errors() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "0".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "0".into() },
                    OpResult::Timeout,
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Read { key: "0".into() },
                    OpResult::Err("connection lost".into()),
                ),
            ],
        };
        ForwardProbeWorkload
            .check_invariant(&history)
            .expect("timeouts/errors must not trip the invariant");
    }
}
