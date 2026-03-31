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

    // -----------------------------------------------------------------------
    // JP-001: Register workload operation generation unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn register_workload_name() {
        let wl = RegisterWorkload;
        assert_eq!(wl.name(), "register");
    }

    /// Run the register workload against a mock session and verify all
    /// generated operations target key "0" and use only Read/Write/Cas ops.
    #[tokio::test]
    async fn register_workload_generates_valid_ops() {
        use crate::workload::testutil::MockCqlSession;
        use std::time::Duration;

        let session = MockCqlSession::new();
        let wl = RegisterWorkload;

        wl.setup(&session).await.unwrap();

        let mut recorder = HistoryRecorder::new("c1");
        wl.run(&session, &mut recorder, Duration::from_millis(50))
            .await
            .unwrap();

        let history = recorder.finish();
        assert!(
            !history.operations.is_empty(),
            "register workload should generate at least one operation in 50ms"
        );

        for op in &history.operations {
            match &op.op {
                Op::Read { key } => {
                    assert_eq!(key, "0", "register workload reads must target key '0'");
                    assert!(
                        matches!(op.result, OpResult::Value(_)),
                        "read result must be Value variant"
                    );
                }
                Op::Write { key, value } => {
                    assert_eq!(key, "0", "register workload writes must target key '0'");
                    assert!(*value > 0, "write values should be positive counters");
                    assert!(
                        matches!(op.result, OpResult::Ok),
                        "write result against mock should be Ok"
                    );
                }
                Op::Cas {
                    key,
                    expected,
                    value,
                } => {
                    assert_eq!(key, "0", "register workload CAS must target key '0'");
                    assert!(
                        *value > *expected,
                        "CAS should increment: expected={expected}, value={value}"
                    );
                    assert!(
                        matches!(op.result, OpResult::Applied(_)),
                        "CAS result must be Applied variant"
                    );
                }
                other => {
                    panic!("register workload should only generate Read/Write/Cas, got: {other:?}");
                }
            }
        }
    }

    /// Register workload invariant passes on empty history (no operations).
    #[test]
    fn register_invariant_empty_history() {
        let history = History { operations: vec![] };
        let wl = RegisterWorkload;
        assert!(wl.check_invariant(&history).is_ok());
    }

    /// Register workload invariant passes with a single write (no reads).
    #[test]
    fn register_invariant_single_write() {
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Write {
                    key: "0".into(),
                    value: 1,
                },
                OpResult::Ok,
            )],
        };
        let wl = RegisterWorkload;
        assert!(wl.check_invariant(&history).is_ok());
    }

    /// Register workload invariant handles timeout operations gracefully.
    #[test]
    fn register_invariant_with_timeouts() {
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
                    "c2",
                    300,
                    400,
                    Op::Write {
                        key: "0".into(),
                        value: 2,
                    },
                    OpResult::Timeout,
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
        let wl = RegisterWorkload;
        // The timed-out write may or may not have applied, so reading 1 is valid.
        assert!(wl.check_invariant(&history).is_ok());
    }

    /// Register workload invariant handles error operations gracefully.
    #[test]
    fn register_invariant_with_errors() {
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
                    "c2",
                    300,
                    400,
                    Op::Read { key: "0".into() },
                    OpResult::Err("connection refused".into()),
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
        let wl = RegisterWorkload;
        assert!(wl.check_invariant(&history).is_ok());
    }

    /// Register workload with CAS chain: w(0) -> CAS(0->1) -> CAS(1->2) -> r(2).
    #[test]
    fn register_invariant_cas_chain() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "0".into(),
                        value: 0,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Cas {
                        key: "0".into(),
                        expected: 0,
                        value: 1,
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Cas {
                        key: "0".into(),
                        expected: 1,
                        value: 2,
                    },
                    OpResult::Applied(true),
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
        let wl = RegisterWorkload;
        assert!(wl.check_invariant(&history).is_ok());
    }
}
