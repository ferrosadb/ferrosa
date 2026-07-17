use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder, Op, OpResult};

use super::{CqlSession, Workload};

/// Number of bank accounts.
const NUM_ACCOUNTS: i64 = 10;
/// Starting balance per account.
const INITIAL_BALANCE: i64 = 1000;
/// Expected total across all accounts.
const EXPECTED_TOTAL: i64 = NUM_ACCOUNTS * INITIAL_BALANCE;

/// Read one account's balance, `None` if the row is absent/unparseable.
async fn read_balance(session: &dyn CqlSession, id: i64) -> Result<Option<i64>> {
    let rows = session
        .execute(&format!(
            "SELECT balance FROM jepsen.accounts WHERE id = {id}"
        ))
        .await?;
    Ok(rows
        .first()
        .and_then(|r| r.first())
        .and_then(|(_, v)| v.parse::<i64>().ok()))
}

/// Multi-account bank transfer workload.
///
/// Setup: 10 accounts each with balance 1000.
/// Operations: **atomic** transfers between random pairs (one multi-key Accord
/// `BEGIN…COMMIT` transaction — both writes or neither), reads of all balances.
/// Invariant: total balance is conserved (sum == 10000); a partial transfer
/// (e.g. from a node failure mid-commit) would break it.
pub struct BankWorkload;

#[async_trait]
impl Workload for BankWorkload {
    fn name(&self) -> &str {
        "bank"
    }

    /// Bank is a transactional workload: writes record transfer *deltas* and
    /// reads are whole-cluster balance snapshots, which the single-value
    /// register linearizability model cannot represent (and per-key
    /// linearizability is not a guarantee Ferrosa's eventually-consistent base
    /// makes). Correctness is judged by value conservation ([`Self::check_invariant`])
    /// and, for the Accord transaction path, by strict serializability (Elle).
    fn register_linearizable(&self) -> bool {
        false
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
                "CREATE TABLE IF NOT EXISTS jepsen.accounts \
                 (id int PRIMARY KEY, balance bigint)",
            )
            .await?;
        for i in 0..NUM_ACCOUNTS {
            session
                .execute(&format!(
                    "INSERT INTO jepsen.accounts (id, balance) VALUES ({i}, {INITIAL_BALANCE})"
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

        // Pace each iteration. Real ops take milliseconds, but under a
        // partition / dc-slow nemesis a CQL call can fail INSTANTLY — without a
        // floor the loop spins at CPU speed and appends millions of failed ops
        // to the in-memory HistoryRecorder, ballooning RSS ~700 MB/s and OOM-ing
        // the host (this killed the nightly tier-multi-dc runner ~30s into every
        // run for 2+ weeks). 1 ms is far below the natural latency-bound rate, so
        // healthy throughput is unaffected; it only caps the error-spin, keeping
        // the history bounded. Unconditional (not end-of-body) because the error
        // paths `continue`.
        const MIN_ITER: Duration = Duration::from_millis(1);
        while start.elapsed() < duration {
            tokio::time::sleep(MIN_ITER).await;
            let r: f64 = rand::random();
            if r < 0.7 {
                // Transfer between two random accounts.
                let from = (rand::random::<u64>() % NUM_ACCOUNTS as u64) as i64;
                let mut to = (rand::random::<u64>() % NUM_ACCOUNTS as u64) as i64;
                if to == from {
                    to = (from + 1) % NUM_ACCOUNTS;
                }
                let amount = (rand::random::<u64>() % 100 + 1) as i64;

                // Read both balances (outside the transaction). This is an
                // ATOMICITY test: the two writes commit together or not at all
                // (one multi-key Accord transaction), so a PARTIAL transfer —
                // debit without credit, e.g. a node failure mid-COMMIT — breaks
                // total conservation and the checker flags it. Full lost-update
                // serializability (conditional commit) is deferred (t_6edfea95).
                recorder.invoke(Op::Read {
                    key: format!("account-{from}"),
                });
                let from_balance = match read_balance(session, from).await {
                    Ok(v) => {
                        recorder.complete(OpResult::Value(v));
                        v
                    }
                    Err(e) => {
                        recorder.complete(OpResult::Err(e.to_string()));
                        continue;
                    }
                };
                let Some(from_balance) = from_balance else {
                    continue;
                };
                if from_balance < amount {
                    continue;
                }

                recorder.invoke(Op::Read {
                    key: format!("account-{to}"),
                });
                let to_balance = match read_balance(session, to).await {
                    Ok(v) => {
                        recorder.complete(OpResult::Value(v));
                        v
                    }
                    Err(e) => {
                        recorder.complete(OpResult::Err(e.to_string()));
                        continue;
                    }
                };
                let Some(to_balance) = to_balance else {
                    continue;
                };

                // Atomic cross-shard transfer: BEGIN; debit; credit; COMMIT — one
                // multi-key Accord transaction. Both writes land, or neither.
                let new_from = from_balance - amount;
                let new_to = to_balance + amount;
                recorder.invoke(Op::Write {
                    key: format!("account-{to}"),
                    value: amount,
                });
                match session
                    .transaction(&[
                        "BEGIN TRANSACTION".to_string(),
                        format!(
                            "UPDATE jepsen.accounts SET balance = {new_from} WHERE id = {from}"
                        ),
                        format!("UPDATE jepsen.accounts SET balance = {new_to} WHERE id = {to}"),
                        "COMMIT".to_string(),
                    ])
                    .await
                {
                    Ok(()) => recorder.complete(OpResult::Ok),
                    Err(e) => recorder.complete(OpResult::Err(e.to_string())),
                }
            } else {
                // Read all account balances.
                recorder.invoke(Op::SerialRead {
                    key: "all-accounts".into(),
                });
                let mut values = Vec::new();
                let mut had_error = false;
                for i in 0..NUM_ACCOUNTS {
                    match session
                        .execute(&format!(
                            "SELECT balance FROM jepsen.accounts WHERE id = {i}"
                        ))
                        .await
                    {
                        Ok(rows) => {
                            let val = rows
                                .first()
                                .and_then(|r| r.first())
                                .map(|(_, v)| v.clone())
                                .unwrap_or_default();
                            values.push((format!("account-{i}"), val));
                        }
                        Err(e) => {
                            recorder.complete(OpResult::Err(e.to_string()));
                            had_error = true;
                            break;
                        }
                    }
                }
                if !had_error {
                    recorder.complete(OpResult::CurrentValues(values));
                }
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        // For every "read all" result, verify conservation.
        for op in &history.operations {
            if let OpResult::CurrentValues(values) = &op.result {
                let total: i64 = values
                    .iter()
                    .filter_map(|(_, v)| v.parse::<i64>().ok())
                    .sum();
                if total != EXPECTED_TOTAL {
                    anyhow::bail!(
                        "Bank balance not conserved: expected {EXPECTED_TOTAL}, got {total}. \
                         Values: {values:?}"
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::history::Operation;
    use crate::workload::testutil::MockCqlSession;

    fn make_op(client: &str, invoke: u64, complete: u64, op: Op, result: OpResult) -> Operation {
        Operation {
            client_id: client.to_string(),
            invoke_us: invoke,
            complete_us: complete,
            op,
            result,
        }
    }

    /// Verify that BankWorkload.setup() and run() execute against a mock
    /// CQL session without panicking and produce a non-empty history.
    #[tokio::test]
    async fn bank_workload_executes() {
        let session = MockCqlSession::new();
        let workload = BankWorkload;

        workload.setup(&session).await.unwrap();

        let mut recorder = HistoryRecorder::new("test");
        // Short duration to keep the test fast; the loop will still execute
        // multiple iterations before the clock fires.
        workload
            .run(&session, &mut recorder, Duration::from_millis(50))
            .await
            .unwrap();

        let history = recorder.finish();
        assert!(
            !history.operations.is_empty(),
            "run() should have recorded at least one operation"
        );

        // Every recorded operation must have a completed result — no pending ops.
        for op in &history.operations {
            assert!(
                !matches!(op.result, OpResult::Timeout),
                "unexpected Timeout result in mock run"
            );
        }
    }

    #[test]
    fn bank_invariant_conserved() {
        // All-accounts read that sums to 10000.
        let values: Vec<(String, String)> = (0..NUM_ACCOUNTS)
            .map(|i| (format!("account-{i}"), INITIAL_BALANCE.to_string()))
            .collect();

        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::SerialRead {
                    key: "all-accounts".into(),
                },
                OpResult::CurrentValues(values),
            )],
        };

        let wl = BankWorkload;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn bank_invariant_violated() {
        // All-accounts read that sums to 9999 (one account short).
        let mut values: Vec<(String, String)> = (0..NUM_ACCOUNTS)
            .map(|i| (format!("account-{i}"), INITIAL_BALANCE.to_string()))
            .collect();
        // Deduct 1 from account-0 without crediting elsewhere.
        values[0].1 = "999".to_string();

        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::SerialRead {
                    key: "all-accounts".into(),
                },
                OpResult::CurrentValues(values),
            )],
        };

        let wl = BankWorkload;
        assert!(wl.check_invariant(&history).is_err());
    }

    /// Regression guard for the register-model false-positive. A bank history
    /// (delta writes + multi-key snapshot reads) is not a single-value register
    /// history, so the generic linearizability checker reports it as
    /// non-linearizable even when every balance was conserved. Bank therefore
    /// opts out of that check and is judged by conservation (+ strict
    /// serializability via Elle) instead.
    #[test]
    fn bank_opts_out_of_register_linearizability() {
        use crate::checker::check_linearizability;

        let wl = BankWorkload;
        assert!(
            !wl.register_linearizable(),
            "bank must not be gated by the single-value register model"
        );

        // A conserved bank history: the write records the transfer *delta*, and
        // a snapshot read sums to the expected total.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "account-1".into(),
                        value: 37,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::SerialRead {
                        key: "all-accounts".into(),
                    },
                    OpResult::CurrentValues(
                        (0..NUM_ACCOUNTS)
                            .map(|i| (format!("account-{i}"), INITIAL_BALANCE.to_string()))
                            .collect(),
                    ),
                ),
            ],
        };

        // Conservation holds…
        assert!(wl.check_invariant(&history).is_ok());
        // …yet the single-value register model still flags it, which is exactly
        // why the orchestrator must skip linearizability for this workload.
        let lin = check_linearizability(&history);
        assert!(
            lin.iter().any(|r| !r.valid),
            "register model is expected to false-fail a (conserved) bank history"
        );
    }
}
