//! Property-based protocol tests for the Accord protocol (A3.10).
//!
//! Uses the `proptest` crate to verify protocol invariants hold under
//! random inputs and mutations. These tests complement the deterministic
//! scenario tests by exploring a much larger state space.

#[cfg(test)]
mod tests {
    use crate::accord::recovery::{RecoverOKResponse, RecoveryCoordinator, RecoveryDecision};
    use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};
    use ferrosa_common::accord::{
        AcceptedBallot, BallotGenerator, BallotNumber, PromisedBallot, Timestamp, TxnId, TxnPhase,
        TxnState,
    };
    use proptest::prelude::*;
    use std::collections::HashSet;

    // =======================================================================
    // A3.10 — Property-Based Protocol Tests (4 tests)
    // =======================================================================

    // -----------------------------------------------------------------------
    // 1. proptest_ballot_invariant_never_violated
    // -----------------------------------------------------------------------

    // Random ballot mutations never break the accepted <= promised invariant.
    //
    // This test generates random sequences of join_ballot (promise) and
    // accept operations and verifies that the TxnState invariant
    // `accepted_ballot <= max_ballot_seen` is maintained throughout.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]
        #[test]
        fn proptest_ballot_invariant_never_violated(
            t0_micros in 1u64..10_000,
            node in 1u64..5,
            // Generate a sequence of (is_accept: bool, ballot_value: u64) operations.
            operations in prop::collection::vec((any::<bool>(), 1u64..500), 1..50),
        ) {
            let t0 = Timestamp::synthetic(t0_micros);
            let txn_id = TxnId::new(node, t0);
            let mut state = TxnState::new(txn_id, t0);

            for (is_accept, ballot_val) in &operations {
                let bn = BallotNumber(*ballot_val);

                if *is_accept {
                    // Accept: only valid if ballot >= current max_ballot_seen.
                    // Skip if ballot is lower (would violate invariant by design).
                    if bn >= state.max_ballot_seen.0 {
                        let deps = HashSet::new();
                        state.accept(AcceptedBallot(bn), t0, deps);
                    }
                } else {
                    // Join ballot (promise): always safe, only advances max_ballot_seen.
                    state.join_ballot(PromisedBallot(bn));
                }

                // Invariant must hold after every operation.
                prop_assert!(
                    (state.accepted_ballot.0).0 <= (state.max_ballot_seen.0).0,
                    "INVARIANT VIOLATION: accepted_ballot {:?} > max_ballot_seen {:?}",
                    state.accepted_ballot,
                    state.max_ballot_seen,
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // 2. proptest_recovery_always_selects_same_value
    // -----------------------------------------------------------------------

    // Given the same set of RecoverOK responses, recovery always produces
    // the same decision regardless of response ordering.
    //
    // This is the key determinism property: feeding all responses to the
    // recovery coordinator must produce the same outcome regardless of
    // the order they arrive.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]
        #[test]
        fn proptest_recovery_always_selects_same_value(
            t0_micros in 1u64..10_000,
            // Generate 3-5 responses with different accepted ballots.
            response_ballots in prop::collection::vec(0u64..100, 3..=5),
            seed in any::<u64>(),
        ) {
            let t0 = Timestamp::synthetic(t0_micros);
            let txn_id = TxnId::new(1, t0);
            let cluster_size = response_ballots.len();

            // Build responses: each has a unique timestamp derived from its ballot.
            let responses: Vec<RecoverOKResponse> = response_ballots
                .iter()
                .enumerate()
                .map(|(i, &ballot_val)| {
                    let t = Timestamp::synthetic(t0_micros + ballot_val + 1);
                    let mut state = TxnState::new(txn_id, t0);
                    state.t = t;
                    if ballot_val > 0 {
                        let bn = BallotNumber(ballot_val);
                        state.accepted_ballot = AcceptedBallot(bn);
                        state.max_ballot_seen = PromisedBallot(bn);
                        state.phase = TxnPhase::Accepted;
                    }
                    RecoverOKResponse {
                        from: (i + 1) as u64,
                        state,
                        superseding: vec![],
                        waiting: vec![],
                    }
                })
                .collect();

            // Helper: feed ALL responses and return the final decision.
            let run_recovery = |order: &[RecoverOKResponse]| -> RecoveryDecision {
                let gen = BallotGenerator::new();
                let mut coord = RecoveryCoordinator::start_recovery(
                    txn_id, t0, cluster_size, &gen,
                );
                let mut decision = None;
                for resp in order {
                    if let Some(d) = coord.handle_recover_ok(resp.clone()) {
                        decision = Some(d);
                    }
                }
                decision.expect("must decide after all responses")
            };

            // Run recovery in original order (all responses).
            let d1 = run_recovery(&responses);

            // Run recovery in reversed order (all responses).
            let reversed: Vec<_> = responses.iter().rev().cloned().collect();
            let d2 = run_recovery(&reversed);

            // Run recovery with a deterministic rotation.
            let mut rotated = responses.clone();
            let rotate_by = (seed as usize) % rotated.len();
            rotated.rotate_left(rotate_by);
            let d3 = run_recovery(&rotated);

            // All three must produce the same decision.
            prop_assert_eq!(&d1, &d2, "reversed order produced different decision");
            prop_assert_eq!(&d1, &d3, "rotated order produced different decision");
        }
    }

    // -----------------------------------------------------------------------
    // 3. proptest_conflicting_txns_always_in_deps
    // -----------------------------------------------------------------------

    // When two transactions touch the same key, each must appear in the
    // other's dependency set after PreAccept.
    //
    // This is the fundamental conflict detection property of Accord:
    // conflicting transactions always discover each other.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]
        #[test]
        fn proptest_conflicting_txns_always_in_deps(
            t0_a_micros in 1u64..5000,
            t0_b_micros in 5001u64..10_000,
            node_a in 1u64..3,
            node_b in 3u64..5,
        ) {
            let mut cluster = TestCluster::new(5);
            let key = b"conflict_key";

            let t0_a = Timestamp::synthetic(t0_a_micros);
            let txn_a = TxnId::new(node_a, t0_a);

            let t0_b = Timestamp::synthetic(t0_b_micros);
            let txn_b = TxnId::new(node_b, t0_b);

            // Txn A: PreAccept on replica 2.
            cluster.send(TestMessage {
                src: node_a,
                dst: 2,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_a,
                    t0: t0_a,
                    key: key.to_vec(),
                },
            });
            let resp_a = cluster.deliver_next();
            prop_assert_eq!(resp_a.len(), 1);

            // Drain PreAcceptOK for txn_a.
            while cluster.pending_count() > 0 {
                cluster.deliver_next();
            }

            // Txn B: PreAccept on the same replica 2 (same key).
            cluster.send(TestMessage {
                src: node_b,
                dst: 2,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_b,
                    t0: t0_b,
                    key: key.to_vec(),
                },
            });
            let resp_b = cluster.deliver_next();
            prop_assert_eq!(resp_b.len(), 1);

            // Txn B should have txn A in its deps (since A was PreAccepted first).
            match &resp_b[0].payload {
                TestMessagePayload::PreAcceptOK { deps, .. } => {
                    prop_assert!(
                        deps.contains(&txn_a),
                        "txn_b's deps must contain txn_a (conflicting key): deps={:?}",
                        deps
                    );
                }
                other => {
                    prop_assert!(false, "expected PreAcceptOK, got {:?}", other);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // 4. proptest_no_duplicate_timestamps
    // -----------------------------------------------------------------------

    // No two transactions with distinct t0 values touching the same key
    // get the same final execution timestamp after PreAccept.
    //
    // The protocol ensures unique timestamps by bumping past conflicts
    // with a (time, seq, node) triple. We generate distinct t0 values
    // (via hash_set) to ensure each txn starts with a unique proposal.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]
        #[test]
        fn proptest_no_duplicate_timestamps(
            // Generate 3-8 distinct base timestamps.
            base_times in prop::collection::hash_set(1u64..50_000, 3..=8),
        ) {
            let mut sorted_times: Vec<u64> = base_times.into_iter().collect();
            sorted_times.sort();
            let num_txns = sorted_times.len();
            let cluster_size = 5;
            let mut cluster = TestCluster::new(cluster_size);
            let key = b"timestamp_key";

            let mut final_timestamps: Vec<Timestamp> = Vec::with_capacity(num_txns);

            for (i, &time) in sorted_times.iter().enumerate() {
                // Each txn uses a unique node to guarantee unique TxnIds.
                let node = ((i % 4) + 1) as u64; // nodes 1-4
                let t0 = Timestamp { epoch: 0, time, seq: i as u32, node };
                let txn_id = TxnId::new(node, t0);

                // PreAccept on replica 5 (always a non-coordinator).
                cluster.send(TestMessage {
                    src: node,
                    dst: 5,
                    payload: TestMessagePayload::PreAccept {
                        txn_id,
                        t0,
                        key: key.to_vec(),
                    },
                });

                let responses = cluster.deliver_next();
                prop_assert_eq!(responses.len(), 1);

                match &responses[0].payload {
                    TestMessagePayload::PreAcceptOK { t, .. } => {
                        final_timestamps.push(*t);
                    }
                    other => {
                        prop_assert!(false, "expected PreAcceptOK, got {:?}", other);
                    }
                }

                // Drain any enqueued responses.
                while cluster.pending_count() > 0 {
                    cluster.deliver_next();
                }
            }

            // Verify no duplicates: all final timestamps must be unique.
            let unique: HashSet<Timestamp> = final_timestamps.iter().copied().collect();
            prop_assert_eq!(
                unique.len(),
                final_timestamps.len(),
                "found duplicate timestamps among {} transactions: {:?}",
                num_txns,
                final_timestamps,
            );
        }
    }
}
