use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder, Op, OpResult};

use super::{CqlSession, Workload};

/// Single-key register workload.
///
/// Concurrent reads, writes, and CAS operations to key "0".
/// Invariant: the history must be linearizable.
pub struct RegisterWorkload;

#[async_trait]
impl Workload for RegisterWorkload {
    fn name(&self) -> &str {
        "register"
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
                "CREATE TABLE IF NOT EXISTS jepsen.register \
                 (id int PRIMARY KEY, val int)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.register (id, val) VALUES (0, 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter = 1i64;

        while start.elapsed() < duration {
            // Random mix: 50% reads, 30% writes, 20% CAS
            let r: f64 = rand::random();
            if r < 0.5 {
                recorder.invoke(Op::Read { key: "0".into() });
                match session
                    .execute("SELECT val FROM jepsen.register WHERE id = 0")
                    .await
                {
                    Ok(rows) => {
                        let val = rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|(_, v)| v.parse().ok());
                        recorder.complete(OpResult::Value(val));
                    }
                    Err(e) => recorder.complete(OpResult::Err(e.to_string())),
                }
            } else if r < 0.8 {
                recorder.invoke(Op::Write {
                    key: "0".into(),
                    value: counter,
                });
                match session
                    .execute(&format!(
                        "UPDATE jepsen.register SET val = {counter} WHERE id = 0"
                    ))
                    .await
                {
                    Ok(_) => recorder.complete(OpResult::Ok),
                    Err(e) => recorder.complete(OpResult::Err(e.to_string())),
                }
                counter += 1;
            } else {
                let expected = counter - 1;
                recorder.invoke(Op::Cas {
                    key: "0".into(),
                    expected,
                    value: counter,
                });
                match session
                    .execute(&format!(
                        "UPDATE jepsen.register SET val = {counter} WHERE id = 0 \
                         IF val = {expected}"
                    ))
                    .await
                {
                    Ok(rows) => {
                        let applied = rows
                            .first()
                            .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                            .unwrap_or(false);
                        recorder.complete(OpResult::Applied(applied));
                    }
                    Err(e) => recorder.complete(OpResult::Err(e.to_string())),
                }
                counter += 1;
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        let results = crate::checker::check_linearizability(history);
        for r in &results {
            if !r.valid {
                anyhow::bail!(
                    "Register not linearizable for key {}: {:?}",
                    r.key,
                    r.counterexample
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::Operation;

    /// Helper: build an Operation with explicit timestamps.
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
    fn register_invariant_linearizable() {
        // Build a simple linearizable history: w(0, 1) then r(0) = 1.
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
            ],
        };

        let wl = RegisterWorkload;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn register_invariant_not_linearizable() {
        // w(0, 1) at [100, 200], w(0, 2) at [300, 400], r(0) = 1 at [500, 600]
        // Stale read after two sequential writes.
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
                    Op::Write {
                        key: "0".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Read { key: "0".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let wl = RegisterWorkload;
        assert!(wl.check_invariant(&history).is_err());
    }
}
