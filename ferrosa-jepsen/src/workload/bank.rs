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

/// Multi-account bank transfer workload.
///
/// Setup: 10 accounts each with balance 1000.
/// Operations: transfers between random pairs (via LWT), reads of all balances.
/// Invariant: total balance is conserved (sum == 10000).
pub struct BankWorkload;

#[async_trait]
impl Workload for BankWorkload {
    fn name(&self) -> &str {
        "bank"
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

        while start.elapsed() < duration {
            let r: f64 = rand::random();
            if r < 0.7 {
                // Transfer between two random accounts.
                let from = (rand::random::<u64>() % NUM_ACCOUNTS as u64) as i64;
                let mut to = (rand::random::<u64>() % NUM_ACCOUNTS as u64) as i64;
                if to == from {
                    to = (from + 1) % NUM_ACCOUNTS;
                }
                let amount = (rand::random::<u64>() % 100 + 1) as i64;

                // Read source balance first.
                recorder.invoke(Op::Read {
                    key: format!("account-{from}"),
                });
                let balance = match session
                    .execute(&format!(
                        "SELECT balance FROM jepsen.accounts WHERE id = {from}"
                    ))
                    .await
                {
                    Ok(rows) => {
                        let val = rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|(_, v)| v.parse::<i64>().ok());
                        recorder.complete(OpResult::Value(val));
                        val
                    }
                    Err(e) => {
                        recorder.complete(OpResult::Err(e.to_string()));
                        continue;
                    }
                };

                let Some(balance) = balance else {
                    continue;
                };

                if balance < amount {
                    continue;
                }

                // CAS debit from source.
                let new_balance = balance - amount;
                recorder.invoke(Op::Cas {
                    key: format!("account-{from}"),
                    expected: balance,
                    value: new_balance,
                });
                let debit_ok = match session
                    .execute(&format!(
                        "UPDATE jepsen.accounts SET balance = {new_balance} \
                         WHERE id = {from} IF balance = {balance}"
                    ))
                    .await
                {
                    Ok(rows) => {
                        let applied = rows
                            .first()
                            .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                            .unwrap_or(false);
                        recorder.complete(OpResult::Applied(applied));
                        applied
                    }
                    Err(e) => {
                        recorder.complete(OpResult::Err(e.to_string()));
                        false
                    }
                };

                if !debit_ok {
                    continue;
                }

                // Credit destination (unconditional, transfer is committed).
                recorder.invoke(Op::Write {
                    key: format!("account-{to}"),
                    value: amount,
                });
                match session
                    .execute(&format!(
                        "UPDATE jepsen.accounts SET balance = balance + {amount} \
                         WHERE id = {to}"
                    ))
                    .await
                {
                    Ok(_) => recorder.complete(OpResult::Ok),
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
}
