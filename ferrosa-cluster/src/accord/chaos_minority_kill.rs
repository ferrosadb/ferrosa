//! Chaos testing: minority kill during active transactions.
//!
//! Simulates killing a minority of replicas during active Accord transactions
//! using the TestCluster deterministic harness. Verifies:
//!
//! - All committed transactions remain durable after minority failure
//! - Recovery completes within a bounded number of steps
//!
//! # A7.8 Tests
//!
//! - `chaos_minority_kill_no_lost_commits` — committed txns survive minority kill
//! - `chaos_minority_kill_recovery_time` — recovery within bounded steps

#[cfg(test)]
mod tests {
    use ferrosa_common::accord::{Timestamp, TxnId, TxnPhase};

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

    /// Run a full PreAccept -> Commit cycle for a transaction.
    fn commit_txn(
        cluster: &mut TestCluster,
        coordinator: u64,
        replicas: &[u64],
        t0_micros: u64,
        key: &[u8],
    ) -> TxnId {
        let t0 = ts(t0_micros);
        let tid = txn(coordinator, t0_micros);

        // PreAccept to all replicas.
        for &r in replicas {
            cluster.send(TestMessage {
                src: coordinator,
                dst: r,
                payload: TestMessagePayload::PreAccept {
                    txn_id: tid,
                    t0,
                    key: key.to_vec(),
                },
            });
        }
        cluster.drain();

        // Determine committed timestamp.
        let max_t = cluster
            .replicas
            .iter()
            .filter_map(|r| r.txn_states.get(&tid))
            .map(|s| s.t)
            .max()
            .unwrap_or(t0);

        // Commit to all replicas.
        for &r in replicas {
            cluster.send(TestMessage {
                src: coordinator,
                dst: r,
                payload: TestMessagePayload::Commit {
                    txn_id: tid,
                    t0,
                    t: max_t,
                    deps: vec![],
                },
            });
        }
        cluster.drain();

        tid
    }

    // =======================================================================
    // A7.8-T1: chaos_minority_kill_no_lost_commits
    // =======================================================================

    /// Kill minority during active transactions; verify all committed
    /// transactions are durable on the surviving majority.
    ///
    /// Setup: 5-node cluster, commit 5 transactions, then simulate killing
    /// 2 nodes (minority). The 3 surviving nodes must retain all committed
    /// state.
    #[test]
    fn chaos_minority_kill_no_lost_commits() {
        let mut cluster = TestCluster::new(5);
        let all_replicas = vec![1, 2, 3, 4, 5];

        // Phase 1: Commit 5 transactions across all 5 replicas.
        let mut committed_txns: Vec<TxnId> = Vec::new();
        for i in 0..5u64 {
            let tid = commit_txn(
                &mut cluster,
                1,
                &all_replicas,
                1000 + i * 100,
                format!("key:{}", i).as_bytes(),
            );
            committed_txns.push(tid);
        }

        // Verify all 5 transactions committed on all replicas.
        for &tid in &committed_txns {
            for replica in &cluster.replicas {
                let state = replica.txn_states.get(&tid).unwrap_or_else(|| {
                    panic!("node {} missing state for {:?}", replica.node_id, tid)
                });
                assert_eq!(
                    state.phase,
                    TxnPhase::Committed,
                    "node {} txn {:?} should be Committed",
                    replica.node_id,
                    tid
                );
            }
        }

        // Phase 2: Kill minority (nodes 4 and 5).
        // In the deterministic harness, "killing" means we don't deliver
        // messages to them and don't check their state.
        let surviving_nodes = vec![1u64, 2, 3];

        // Phase 3: Submit new transactions to surviving majority only.
        let post_kill_tid = commit_txn(&mut cluster, 1, &surviving_nodes, 2000, b"key:post-kill");

        // Phase 4: Verify all previously committed transactions are still
        // present on the surviving majority.
        for &tid in &committed_txns {
            let mut surviving_committed_count = 0;
            for &node_id in &surviving_nodes {
                let replica = cluster.replica(node_id);
                if let Some(state) = replica.txn_states.get(&tid) {
                    assert_eq!(
                        state.phase,
                        TxnPhase::Committed,
                        "surviving node {} txn {:?} must still be Committed",
                        node_id,
                        tid
                    );
                    surviving_committed_count += 1;
                }
            }
            // All 3 surviving nodes must have the committed state.
            assert_eq!(
                surviving_committed_count, 3,
                "all 3 surviving nodes must retain committed txn {:?}",
                tid
            );
        }

        // The new post-kill transaction must also be committed on survivors.
        for &node_id in &surviving_nodes {
            let replica = cluster.replica(node_id);
            let state = replica
                .txn_states
                .get(&post_kill_tid)
                .expect("surviving node must have post-kill txn");
            assert_eq!(
                state.phase,
                TxnPhase::Committed,
                "post-kill txn must be Committed on node {}",
                node_id,
            );
        }

        // Consistency check on all committed txns among survivors.
        for &tid in &committed_txns {
            cluster.assert_consistent(&tid);
        }
        cluster.assert_consistent(&post_kill_tid);
    }

    // =======================================================================
    // A7.8-T2: chaos_minority_kill_recovery_time
    // =======================================================================

    /// Recovery after minority kill completes within bounded steps.
    ///
    /// The deterministic equivalent of "30 seconds" is a bounded number of
    /// message deliveries. After killing 2 of 5 nodes during in-flight
    /// transactions, recovery (re-sending PreAccept/Commit to surviving
    /// majority) must complete within MAX_RECOVERY_STEPS deliveries.
    #[test]
    fn chaos_minority_kill_recovery_time() {
        // Deterministic equivalent of 30s timeout: bounded message deliveries.
        const MAX_RECOVERY_STEPS: usize = 200;

        let mut cluster = TestCluster::new(5);
        let all_replicas = vec![1, 2, 3, 4, 5];
        let surviving_nodes = vec![1u64, 2, 3];

        // Start a transaction across all 5 nodes.
        let t0 = ts(5000);
        let tid = txn(1, 5000);

        // PreAccept to all 5.
        for &r in &all_replicas {
            cluster.send(TestMessage {
                src: 1,
                dst: r,
                payload: TestMessagePayload::PreAccept {
                    txn_id: tid,
                    t0,
                    key: b"recovery:key".to_vec(),
                },
            });
        }

        // Kill nodes 4 and 5 by dropping their messages.
        // Messages are queued: [to:1, to:2, to:3, to:4, to:5]
        // Drop messages to nodes 4 and 5 (indices 3 and 4, but after
        // dropping index 3, the old index 4 becomes index 3).
        cluster.drop_at(3);
        cluster.drop_at(3);

        // Track steps for recovery.
        let mut recovery_steps: usize = 0;

        // Deliver remaining PreAccepts to surviving nodes.
        while cluster.pending_count() > 0 && recovery_steps < MAX_RECOVERY_STEPS {
            cluster.deliver_next();
            recovery_steps += 1;
        }

        // Now simulate recovery: coordinator detects nodes 4,5 are dead
        // and re-commits to the surviving majority.
        let max_t = cluster
            .replicas
            .iter()
            .filter_map(|r| r.txn_states.get(&tid))
            .map(|s| s.t)
            .max()
            .unwrap_or(t0);

        // Send Commit to surviving nodes only.
        for &r in &surviving_nodes {
            cluster.send(TestMessage {
                src: 1,
                dst: r,
                payload: TestMessagePayload::Commit {
                    txn_id: tid,
                    t0,
                    t: max_t,
                    deps: vec![],
                },
            });
        }

        // Deliver commits.
        while cluster.pending_count() > 0 && recovery_steps < MAX_RECOVERY_STEPS {
            cluster.deliver_next();
            recovery_steps += 1;
        }

        // Verify recovery completed within the bound.
        assert!(
            recovery_steps < MAX_RECOVERY_STEPS,
            "recovery took {} steps, exceeding bound of {}",
            recovery_steps,
            MAX_RECOVERY_STEPS,
        );

        // Verify the transaction is committed on all surviving nodes.
        for &node_id in &surviving_nodes {
            let replica = cluster.replica(node_id);
            let state = replica
                .txn_states
                .get(&tid)
                .unwrap_or_else(|| panic!("surviving node {} must have txn state", node_id));
            assert_eq!(
                state.phase,
                TxnPhase::Committed,
                "node {} txn must be Committed after recovery (step {})",
                node_id,
                recovery_steps,
            );
        }

        // Consistency across survivors.
        cluster.assert_consistent(&tid);

        // Verify nodes 4 and 5 are NOT committed (they were killed).
        for &dead_node in &[4u64, 5] {
            let replica = cluster.replica(dead_node);
            // Dead nodes never received PreAccept so they have no state.
            let is_uncommitted = match replica.txn_states.get(&tid) {
                None => true,
                Some(state) => state.phase != TxnPhase::Committed,
            };
            assert!(
                is_uncommitted,
                "dead node {} must not have committed txn",
                dead_node,
            );
        }
    }
}
