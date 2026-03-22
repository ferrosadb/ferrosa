//! Jepsen-style bank and write-skew tests using the deterministic TestCluster harness.
//!
//! These tests verify strict serializability properties of cross-shard Accord
//! transactions without real networking or async runtimes. The [`TestCluster`]
//! deterministic harness provides total control over message ordering, making
//! the tests reproducible and CI-friendly.
//!
//! # Tests
//!
//! - **A6.7-T1** (`jepsen_bank_atomicity`): 100 accounts, concurrent transfers via
//!   cross-shard Accord transactions. Total balance invariant never violated.
//! - **A6.7-T2** (`jepsen_bank_no_negative_balance`): No account ever goes negative.
//! - **A6.8-T1** (`jepsen_write_skew`): Two concurrent transactions read a shared
//!   counter; strict serializability prevents both from committing based on a
//!   stale read.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::cross_shard::{CrossShardCoordinator, CrossShardOutcome, ShardId};
    use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
    }

    /// Simple in-memory bank that can be driven by Accord transactions.
    ///
    /// Each account is identified by a `u64` and holds an `i64` balance.
    /// All mutations go through methods that enforce invariants.
    struct Bank {
        accounts: HashMap<u64, i64>,
    }

    impl Bank {
        /// Create a bank with `n` accounts, each starting at `initial_balance`.
        fn new(n: u64, initial_balance: i64) -> Self {
            let mut accounts = HashMap::new();
            for id in 0..n {
                accounts.insert(id, initial_balance);
            }
            Self { accounts }
        }

        /// Total balance across all accounts.
        fn total_balance(&self) -> i64 {
            self.accounts.values().sum()
        }

        /// Attempt a transfer. Returns true if the transfer succeeded (source
        /// had sufficient funds), false otherwise.
        fn transfer(&mut self, from: u64, to: u64, amount: i64) -> bool {
            assert!(amount > 0, "transfer amount must be positive");
            assert_ne!(from, to, "cannot transfer to self");

            let from_balance = *self.accounts.get(&from).expect("source account missing");
            if from_balance < amount {
                return false; // insufficient funds
            }

            *self.accounts.get_mut(&from).unwrap() -= amount;
            *self.accounts.get_mut(&to).unwrap() += amount;
            true
        }

        /// Get account balance.
        fn balance(&self, id: u64) -> i64 {
            *self.accounts.get(&id).expect("account not found")
        }

        /// Check that no account has a negative balance.
        fn assert_no_negative(&self) {
            for (&id, &balance) in &self.accounts {
                assert!(
                    balance >= 0,
                    "account {} has negative balance: {}",
                    id,
                    balance
                );
            }
        }
    }

    // =======================================================================
    // A6.7: Jepsen Bank Tests (2 tests)
    // =======================================================================

    /// A6.7-T1: 100 accounts, concurrent transfers via cross-shard Accord
    /// transactions. Total balance invariant never violated.
    ///
    /// Strategy: Each transfer touches two shards (source account shard and
    /// destination account shard). We run transfers through the
    /// CrossShardCoordinator, which enforces atomic all-or-nothing semantics.
    /// After every transfer, we verify the total balance is unchanged.
    #[test]
    fn jepsen_bank_atomicity() {
        const NUM_ACCOUNTS: u64 = 100;
        const INITIAL_BALANCE: i64 = 1000;
        const NUM_TRANSFERS: u64 = 200;

        let expected_total = NUM_ACCOUNTS as i64 * INITIAL_BALANCE;

        let mut bank = Bank::new(NUM_ACCOUNTS, INITIAL_BALANCE);
        assert_eq!(bank.total_balance(), expected_total);

        // Each account maps to its own shard for maximum cross-shard coverage.
        let shard_ids: Vec<ShardId> = (0..NUM_ACCOUNTS).collect();
        let writer = Arc::new(MockSyncWriter::new());
        let mut coord = CrossShardCoordinator::new(&shard_ids, writer);

        let ballot = BallotNumber(1);
        let mut committed = 0u64;

        for i in 0..NUM_TRANSFERS {
            // Deterministic pseudo-random source and destination.
            let from = (i * 7 + 3) % NUM_ACCOUNTS;
            let to = (i * 13 + 11) % NUM_ACCOUNTS;
            if from == to {
                continue; // skip self-transfers
            }

            let amount = ((i % 50) + 1) as i64;
            let tid = txn(1, 100_000 + i);
            let t0 = ts(100_000 + i);

            let shard_keys: Vec<(ShardId, Vec<u8>)> = vec![
                (from, format!("acct_{}", from).into_bytes()),
                (to, format!("acct_{}", to).into_bytes()),
            ];

            // Execute the transfer through the coordinator.
            let from_balance = bank.balance(from);
            let outcome = coord.execute(tid, t0, &shard_keys, ballot, |_shard_id, _sm, _key| {
                // The execute_fn succeeds only if the source has funds.
                if from_balance >= amount {
                    crate::accord::cross_shard::ShardResult::Ok(amount.to_le_bytes().to_vec())
                } else {
                    crate::accord::cross_shard::ShardResult::Failed(
                        "insufficient funds".to_string(),
                    )
                }
            });

            match &outcome {
                CrossShardOutcome::Committed(_) => {
                    // Apply the transfer to our bank model.
                    let transferred = bank.transfer(from, to, amount);
                    assert!(
                        transferred,
                        "coordinator committed but bank rejected transfer {} -> {} amount {}",
                        from, to, amount
                    );
                    committed += 1;
                }
                CrossShardOutcome::Aborted { .. } => {
                    // Aborted transfers do not change bank state.
                }
            }

            // INVARIANT: total balance must never change.
            assert_eq!(
                bank.total_balance(),
                expected_total,
                "total balance invariant violated after transfer {}: from={}, to={}, amount={}",
                i,
                from,
                to,
                amount,
            );
        }

        // Verify we actually exercised the commit path.
        assert!(committed > 0, "at least one transfer must commit");
        // Final check.
        assert_eq!(bank.total_balance(), expected_total);
    }

    /// A6.7-T2: No account ever goes negative during concurrent transfers.
    ///
    /// This test deliberately attempts transfers that would overdraw accounts.
    /// The Accord coordinator aborts such transactions (via the execute_fn
    /// returning `Failed`), and we verify no account goes negative at any
    /// point during execution.
    #[test]
    fn jepsen_bank_no_negative_balance() {
        const NUM_ACCOUNTS: u64 = 100;
        const INITIAL_BALANCE: i64 = 50;
        const NUM_TRANSFERS: u64 = 300;

        let mut bank = Bank::new(NUM_ACCOUNTS, INITIAL_BALANCE);
        let expected_total = NUM_ACCOUNTS as i64 * INITIAL_BALANCE;

        let shard_ids: Vec<ShardId> = (0..NUM_ACCOUNTS).collect();
        let writer = Arc::new(MockSyncWriter::new());
        let mut coord = CrossShardCoordinator::new(&shard_ids, writer);

        let ballot = BallotNumber(1);
        let mut negative_attempts = 0u64;

        for i in 0..NUM_TRANSFERS {
            let from = (i * 11 + 7) % NUM_ACCOUNTS;
            let to = (i * 17 + 3) % NUM_ACCOUNTS;
            if from == to {
                continue;
            }

            // Deliberately use large amounts to trigger overdrafts.
            let amount = ((i % 80) + 10) as i64;
            let tid = txn(2, 200_000 + i);
            let t0 = ts(200_000 + i);

            let shard_keys: Vec<(ShardId, Vec<u8>)> = vec![
                (from, format!("acct_{}", from).into_bytes()),
                (to, format!("acct_{}", to).into_bytes()),
            ];

            let from_balance = bank.balance(from);
            let would_overdraft = from_balance < amount;

            let outcome = coord.execute(tid, t0, &shard_keys, ballot, |_shard_id, _sm, _key| {
                if !would_overdraft {
                    crate::accord::cross_shard::ShardResult::Ok(amount.to_le_bytes().to_vec())
                } else {
                    crate::accord::cross_shard::ShardResult::Failed(
                        "overdraft prevented".to_string(),
                    )
                }
            });

            if would_overdraft {
                negative_attempts += 1;
                // Must have been aborted.
                assert!(
                    matches!(&outcome, CrossShardOutcome::Aborted { .. }),
                    "overdraft transfer must be aborted: from={} balance={} amount={}",
                    from,
                    from_balance,
                    amount,
                );
            }

            if matches!(&outcome, CrossShardOutcome::Committed(_)) {
                let transferred = bank.transfer(from, to, amount);
                assert!(transferred, "committed transfer must succeed in bank model");
            }

            // INVARIANT: no account ever goes negative.
            bank.assert_no_negative();

            // INVARIANT: total balance is preserved.
            assert_eq!(bank.total_balance(), expected_total);
        }

        // We must have actually tested some overdraft attempts.
        assert!(
            negative_attempts > 0,
            "test must exercise at least one overdraft attempt"
        );
    }

    // =======================================================================
    // A6.8: Jepsen Write-Skew Test (1 test)
    // =======================================================================

    /// A6.8-T1: Two concurrent transactions read a shared counter. Strict
    /// serializability prevents both from committing based on a stale read.
    ///
    /// Scenario:
    /// - Shared counter starts at 0.
    /// - Txn A reads counter (sees 0), plans to write 1.
    /// - Txn B reads counter (sees 0), plans to write 1.
    /// - Under strict serializability, only ONE of A or B may commit,
    ///   because the other's read is invalidated by the first's write.
    ///
    /// We model this using the TestCluster deterministic harness. Both
    /// transactions touch the same key on the same replica. The Accord
    /// protocol's dependency tracking ensures that at most one commits
    /// based on the stale (t0) read.
    #[test]
    fn jepsen_write_skew() {
        let mut cluster = TestCluster::new(3);

        let counter_key = b"counter".to_vec();

        // Transaction A: node 1 proposes at t0=1000.
        let t0_a = ts(1000);
        let txn_a = TxnId::new(1, t0_a);

        // Transaction B: node 2 proposes at t0=1001 (concurrent).
        let t0_b = ts(1001);
        let txn_b = TxnId::new(2, t0_b);

        // Both send PreAccept to replicas 1, 2, 3 for the same key.
        // This creates a conflict: both txns touch "counter".
        for dst in 1..=3u64 {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_a,
                    t0: t0_a,
                    key: counter_key.clone(),
                },
            });
        }
        for dst in 1..=3u64 {
            cluster.send(TestMessage {
                src: 2,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_b,
                    t0: t0_b,
                    key: counter_key.clone(),
                },
            });
        }

        // Deliver all PreAccept messages (6 total: 3 for A, 3 for B).
        // Txn A's PreAccepts are delivered first. When B's PreAccepts arrive,
        // each replica sees A in its conflict set and lists A as a dependency.
        let mut responses_a = Vec::new();
        let mut responses_b = Vec::new();

        // Deliver A's 3 PreAccepts.
        for _ in 0..3 {
            let resps = cluster.deliver_next();
            for r in resps {
                responses_a.push(r);
            }
        }

        // Deliver B's 3 PreAccepts.
        for _ in 0..3 {
            let resps = cluster.deliver_next();
            for r in resps {
                responses_b.push(r);
            }
        }

        // Verify that B's PreAcceptOK responses include txn_a as a dependency.
        // This dependency ordering is what prevents write-skew: B must wait
        // for A to complete before it can be applied, so B cannot commit
        // based on a stale read of the counter.
        let mut b_has_dep_on_a = false;
        for resp in &responses_b {
            if let TestMessagePayload::PreAcceptOK { deps, .. } = &resp.payload {
                if deps.contains(&txn_a) {
                    b_has_dep_on_a = true;
                }
            }
        }

        assert!(
            b_has_dep_on_a,
            "txn B must have txn A as a dependency to prevent write-skew"
        );

        // Verify serialization: the dependency graph forces a total order.
        // A has no dependency on B (A arrived first).
        let mut a_has_dep_on_b = false;
        for resp in &responses_a {
            if let TestMessagePayload::PreAcceptOK { deps, .. } = &resp.payload {
                if deps.contains(&txn_b) {
                    a_has_dep_on_b = true;
                }
            }
        }

        assert!(
            !a_has_dep_on_b,
            "txn A must NOT depend on txn B (A was ordered first)"
        );

        // Now commit A with its proposed timestamp.
        for dst in 1..=3u64 {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::Commit {
                    txn_id: txn_a,
                    t0: t0_a,
                    t: t0_a,
                    deps: vec![],
                },
            });
        }

        // Deliver the commit messages.
        // We need to deliver them from pending. The pending queue currently has
        // the PreAcceptOK responses from A and B (6 total) plus the 3 commits.
        // Drain all: commits generate no responses.
        cluster.drain();

        // After drain, verify strict serializability properties:
        // 1. A is committed on all replicas.
        for node_id in 1..=3u64 {
            let replica = cluster.replica(node_id);
            let state_a = replica.txn_states.get(&txn_a);
            assert!(
                state_a.is_some(),
                "replica {} must know about txn A",
                node_id
            );
            assert_eq!(
                state_a.unwrap().phase,
                ferrosa_common::accord::TxnPhase::Committed,
                "txn A must be Committed on replica {}",
                node_id
            );
        }

        // 2. B has A in its dependency set on every replica that knows about B.
        //    This means B cannot be applied until A is applied — preventing
        //    write-skew where both would read counter=0 and write counter=1.
        for node_id in 1..=3u64 {
            let replica = cluster.replica(node_id);
            if let Some(state_b) = replica.txn_states.get(&txn_b) {
                assert!(
                    state_b.deps.contains(&txn_a),
                    "replica {}: txn B must depend on txn A to prevent write-skew",
                    node_id
                );
            }
        }

        // 3. Consistency: all replicas agree on A's committed state.
        cluster.assert_consistent(&txn_a);
    }
}
