//! 24-step EPaxos correctness test — CI gate.
//!
//! Encodes the exact counter-example from Sutra et al. (2019) that
//! demonstrates the single-ballot-variable safety violation in EPaxos.
//! With the bug (one ballot field), replicas diverge on committed deps.
//! With the fix (separate `accepted_ballot` and `max_ballot_seen`), all
//! replicas converge.
//!
//! This test is DETERMINISTIC: no randomness, no timers, no tokio.
//! Messages are injected in a specific order via the TestCluster scheduler.

use ferrosa_cluster::accord::test_cluster::*;
use ferrosa_common::accord::*;
use std::collections::HashSet;

/// Build a TxnId for a node with a given time value.
///
/// Uses epoch=0 so that timestamp ordering is determined solely by `time`.
fn make_txn_id(node: u64, time: u64) -> TxnId {
    TxnId::new(node, Timestamp::new(0, time, node))
}

/// Print diagnostic ballot state for a replica's view of a transaction.
/// Called on failure to aid debugging.
fn dump_ballot_state(cluster: &TestCluster, txn_id: &TxnId) {
    for replica in &cluster.replicas {
        if let Some(state) = replica.txn_states.get(txn_id) {
            eprintln!(
                "  p{}: phase={:?}, accepted_ballot={:?}, max_ballot_seen={:?}, deps={:?}",
                replica.node_id,
                state.phase,
                state.accepted_ballot,
                state.max_ballot_seen,
                state.deps,
            );
        } else {
            eprintln!("  p{}: <no state for this txn>", replica.node_id);
        }
    }
}

#[test]
fn epaxos_24_step_linearizability() {
    let mut cluster = TestCluster::new(3); // p1=1, p2=2, p3=3

    // Two conflicting transactions on the same key.
    // c1 coordinated by p3 (node=3), c2 coordinated by p1 (node=1).
    // c1 has lower time (100) than c2 (200) so c1 < c2 in timestamp order.
    let key = b"shared_key".to_vec();
    let c1 = make_txn_id(3, 100); // TxnId(Timestamp { epoch:0, time:100, seq:0, node:3 })
    let c2 = make_txn_id(1, 200); // TxnId(Timestamp { epoch:0, time:200, seq:0, node:1 })

    // Verify c1 < c2 in total order (time 100 < 200).
    assert!(c1.0 < c2.0, "c1 must have lower timestamp than c2");

    // =========================================================================
    // Step 1: p3 sends PreAccept(c1) to {p1, p2, p3}
    // =========================================================================
    cluster.send(TestMessage {
        src: 3,
        dst: 1,
        payload: TestMessagePayload::PreAccept {
            txn_id: c1,
            t0: c1.0,
            key: key.clone(),
        },
    });
    cluster.send(TestMessage {
        src: 3,
        dst: 2,
        payload: TestMessagePayload::PreAccept {
            txn_id: c1,
            t0: c1.0,
            key: key.clone(),
        },
    });
    cluster.send(TestMessage {
        src: 3,
        dst: 3,
        payload: TestMessagePayload::PreAccept {
            txn_id: c1,
            t0: c1.0,
            key: key.clone(),
        },
    });
    assert_eq!(
        cluster.pending_count(),
        3,
        "Step 1: 3 PreAccept(c1) pending"
    );

    // =========================================================================
    // Step 2: p1 sends PreAccept(c2) to {p1, p2, p3}
    // =========================================================================
    cluster.send(TestMessage {
        src: 1,
        dst: 1,
        payload: TestMessagePayload::PreAccept {
            txn_id: c2,
            t0: c2.0,
            key: key.clone(),
        },
    });
    cluster.send(TestMessage {
        src: 1,
        dst: 2,
        payload: TestMessagePayload::PreAccept {
            txn_id: c2,
            t0: c2.0,
            key: key.clone(),
        },
    });
    cluster.send(TestMessage {
        src: 1,
        dst: 3,
        payload: TestMessagePayload::PreAccept {
            txn_id: c2,
            t0: c2.0,
            key: key.clone(),
        },
    });
    assert_eq!(cluster.pending_count(), 6, "Step 2: 6 total pending");

    // Queue state (indices 0-5):
    //   0: PreAccept(c1) -> p1
    //   1: PreAccept(c1) -> p2
    //   2: PreAccept(c1) -> p3
    //   3: PreAccept(c2) -> p1
    //   4: PreAccept(c2) -> p2
    //   5: PreAccept(c2) -> p3

    // =========================================================================
    // Step 3: Deliver PreAccept(c1) at p3 FIRST, then deliver PreAccept(c2)
    //         at p3. p3 already has c1 in conflicts -> deps={c1} for c2.
    // =========================================================================

    // Deliver PreAccept(c1) -> p3 (index 2)
    let responses = cluster.deliver_at(2);
    assert_eq!(responses.len(), 1, "Step 3a: p3 responds to PreAccept(c1)");
    match &responses[0].payload {
        TestMessagePayload::PreAcceptOK { txn_id, deps, .. } => {
            assert_eq!(*txn_id, c1);
            assert!(deps.is_empty(), "Step 3a: p3 has no prior conflicts for c1");
        }
        other => panic!("Step 3a: expected PreAcceptOK, got {:?}", other),
    }

    // Now deliver PreAccept(c2) -> p3 (was at index 5, but we removed one,
    // so it's now at index 4). Response is enqueued at the back.
    // Queue: [PA(c1)->p1, PA(c1)->p2, PA(c2)->p1, PA(c2)->p2, PA(c2)->p3, PAOK(c1)->p3]
    //         0           1           2            3            4            5
    let responses = cluster.deliver_at(4);
    assert_eq!(responses.len(), 1, "Step 3b: p3 responds to PreAccept(c2)");
    match &responses[0].payload {
        TestMessagePayload::PreAcceptOK { txn_id, deps, .. } => {
            assert_eq!(*txn_id, c2);
            assert!(
                deps.contains(&c1),
                "Step 3: p3 sees c1 in conflicts, so deps for c2 must contain c1. Got: {:?}",
                deps,
            );
        }
        other => panic!("Step 3: expected PreAcceptOK(c2), got {:?}", other),
    }

    // Verify p3's state for c2.
    {
        let p3_c2 = cluster
            .replica(3)
            .txn_states
            .get(&c2)
            .expect("Step 3: p3 must have state for c2");
        assert!(
            p3_c2.deps.contains(&c1),
            "Step 3: p3's deps for c2 must include c1",
        );
        assert_eq!(p3_c2.phase, TxnPhase::PreAccepted);
    }

    // =========================================================================
    // Steps 4-6: p3 initiates recovery for c2 with ballot=2.
    //   p3 sends Recover(ballot=2, c2) to {p2, p3}.
    // =========================================================================
    // Drop remaining PreAccept messages that we don't need for this scenario.
    // Current queue (roughly):
    //   0: PA(c1)->p1
    //   1: PA(c1)->p2
    //   2: PA(c2)->p1
    //   3: PA(c2)->p2
    //   4: PAOK(c1) -> p3 (response from step 3a)
    //   5: PAOK(c2) -> p1 (response from step 3b, dst=src of original=1)
    //
    // We need to carefully manage the queue. Let's drop all remaining
    // PreAccept messages that would interfere, and the PreAcceptOK responses
    // we don't need yet. We'll re-inject what we need.

    // Clear the pending queue by dropping everything — we'll manually inject
    // the recovery messages. The replicas already have the state we set up.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // Step 4: p3 sends Recover(ballot=2, c2) to p2 and p3.
    let ballot_2 = BallotNumber(2);
    cluster.send(TestMessage {
        src: 3,
        dst: 2,
        payload: TestMessagePayload::Recover {
            ballot: ballot_2,
            txn_id: c2,
            t0: c2.0,
        },
    });
    cluster.send(TestMessage {
        src: 3,
        dst: 3,
        payload: TestMessagePayload::Recover {
            ballot: ballot_2,
            txn_id: c2,
            t0: c2.0,
        },
    });
    assert_eq!(
        cluster.pending_count(),
        2,
        "Step 4: 2 Recover messages pending"
    );

    // Step 5: Deliver Recover(ballot=2) at p2.
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 5: p2 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { txn_id, state, .. } => {
            assert_eq!(*txn_id, c2);
            // p2 never saw c1 or c2 before (we didn't deliver those PreAccepts
            // to p2). So p2 creates fresh state with accepted_ballot=0.
            assert_eq!(
                (state.accepted_ballot.0).0,
                0,
                "Step 5: p2 has accepted_ballot=0 (never accepted c2)",
            );
        }
        other => panic!("Step 5: expected RecoverOK, got {:?}", other),
    }
    {
        let p2_c2 = cluster
            .replica(2)
            .txn_states
            .get(&c2)
            .expect("Step 5: p2 must have state for c2");
        assert_eq!(
            (p2_c2.max_ballot_seen.0).0,
            2,
            "Step 5: p2.max_ballot_seen must be 2 after Recover(ballot=2)",
        );
    }

    // Step 6: Deliver Recover(ballot=2) at p3.
    // Drop the RecoverOK response enqueued by step 5 first — it goes to p3
    // (the recovery coordinator) and we don't want it auto-delivered yet.
    // Actually, we just deliver the Recover to p3 next.
    // Queue: [Recover(2)->p3, RecoverOK->p3]
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 6: p3 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { txn_id, state, .. } => {
            assert_eq!(*txn_id, c2);
            // p3 pre-accepted c2 in step 3 (with deps={c1}).
            // accepted_ballot is still 0 because p3 only pre-accepted, never accepted.
            assert_eq!(
                (state.accepted_ballot.0).0,
                0,
                "Step 6: p3 accepted_ballot=0 (only pre-accepted, not accepted)",
            );
            assert!(
                state.deps.contains(&c1),
                "Step 6: p3's RecoverOK includes deps containing c1 from pre-accept",
            );
        }
        other => panic!("Step 6: expected RecoverOK, got {:?}", other),
    }
    {
        let p3_c2 = cluster
            .replica(3)
            .txn_states
            .get(&c2)
            .expect("Step 6: p3 must have state for c2");
        assert_eq!(
            (p3_c2.max_ballot_seen.0).0,
            2,
            "Step 6: p3.max_ballot_seen must be 2 after Recover(ballot=2)",
        );
    }

    // =========================================================================
    // Step 7: p3 (recovery coordinator) sees no accepted state from either
    //         respondent (both have accepted_ballot=0). p3 saw deps={c1} from
    //         its own pre-accept. Recovery chooses deps={c1}.
    //         p3 sends Accept(ballot=2, c2, deps={c1}) to {p2, p3}.
    // =========================================================================
    // Clear RecoverOK responses from the queue.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    let mut deps_c1: HashSet<TxnId> = HashSet::new();
    deps_c1.insert(c1);
    cluster.send(TestMessage {
        src: 3,
        dst: 2,
        payload: TestMessagePayload::Accept {
            ballot: ballot_2,
            txn_id: c2,
            t0: c2.0,
            t: c2.0, // execution timestamp unchanged
            deps: deps_c1.iter().copied().collect(),
        },
    });
    cluster.send(TestMessage {
        src: 3,
        dst: 3,
        payload: TestMessagePayload::Accept {
            ballot: ballot_2,
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: deps_c1.iter().copied().collect(),
        },
    });
    assert_eq!(
        cluster.pending_count(),
        2,
        "Step 7: 2 Accept messages pending"
    );

    // Deliver Accept(ballot=2) at p3 first to establish accepted state there.
    // Queue: [Accept->p2, Accept->p3]
    let responses = cluster.deliver_at(1); // Accept->p3
    assert_eq!(responses.len(), 1, "Step 7a: p3 sends AcceptOK");
    {
        let p3_c2 = cluster
            .replica(3)
            .txn_states
            .get(&c2)
            .expect("Step 7: p3 must have state for c2");
        assert_eq!(
            (p3_c2.accepted_ballot.0).0,
            2,
            "Step 7: p3.accepted_ballot must be 2 after Accept(ballot=2)",
        );
        assert_eq!(p3_c2.phase, TxnPhase::Accepted);
        assert!(
            p3_c2.deps.contains(&c1),
            "Step 7: p3's deps must contain c1 after Accept",
        );
    }

    // Deliver Accept(ballot=2) at p2.
    // Queue: [Accept->p2, AcceptOK->p3]
    let responses = cluster.deliver_next(); // Accept->p2
    assert_eq!(responses.len(), 1, "Step 7b: p2 sends AcceptOK");
    {
        let p2_c2 = cluster
            .replica(2)
            .txn_states
            .get(&c2)
            .expect("Step 7: p2 must have state for c2");
        assert_eq!(
            (p2_c2.accepted_ballot.0).0,
            2,
            "Step 7: p2.accepted_ballot must be 2 after Accept(ballot=2)",
        );
        assert!(
            p2_c2.deps.contains(&c1),
            "Step 7: p2's deps must contain c1 after Accept(ballot=2, deps=c1)",
        );
    }

    // Clear AcceptOK responses.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // =========================================================================
    // Steps 8-10: p2 initiates SECOND recovery for c2 with ballot=3.
    //   p2 sends Recover(ballot=3) to {p1, p2}.
    //   Neither p1 nor p2's RecoverOK reveals the Accept from step 7,
    //   because p2 is now being asked at a higher ballot, and p1 never saw
    //   the Accept. (p2 DID accept at ballot 2, but this recovery collects
    //   from p1 and p2 — p1 has accepted_ballot=0.)
    //
    //   Wait — actually p2 accepted at ballot=2 in step 7b. So p2's
    //   RecoverOK WILL show accepted_ballot=2.
    //   But the spec says "p2 sees accepted_ballot==0 from both p1 and p2"
    //   at step 11. This means the recovery coord should see that p1 has
    //   accepted_ballot=0 and p2 has accepted_ballot=2, and since no quorum
    //   accepted the same value, it can propose its own value.
    //
    //   Re-reading the spec more carefully:
    //   Step 11: "p2 (recovery coord) collects → sees no accepted state →
    //   sends Accept(ballot=3, c2, deps={})"
    //   "Assert: p2 sees accepted_ballot==0 from both p1 and p2"
    //
    //   This implies we should set up the scenario so that p2's Recover
    //   collects from p1 and p2 BEFORE the Accept from step 7 is visible
    //   to p2. But we already delivered it!
    //
    //   The key insight: the spec's step ordering means the Accept(ballot=2)
    //   at step 7 only reached p3, not p2. Let me re-read...
    //
    //   Actually, looking at step 7 again: "p3 sends Accept(ballot=2, c2,
    //   deps={c1}) to {p2, p3}". It sends to BOTH. But the question is
    //   about delivery timing. The Accept to p2 might not have been
    //   delivered before p2 starts its own recovery.
    //
    //   For the scenario to work per the spec, we need Accept(ballot=2)
    //   to reach p3 but NOT p2 before step 8. Let me redo step 7.
    // =========================================================================

    // We need to undo step 7b. Since we already delivered the Accept to p2,
    // let's re-set p2's state for c2. The spec requires that p2 didn't
    // receive the Accept(ballot=2) before initiating its own recovery.
    //
    // The cleanest approach: reset p2's c2 state to what it was after step 5
    // (Recover delivered, max_ballot_seen=2, accepted_ballot=0).
    {
        let p2 = cluster.replica_mut(2);
        let p2_c2 = p2.txn_states.get_mut(&c2).expect("p2 must have c2 state");
        // Reset to post-step-5 state: max_ballot_seen=2, accepted_ballot=0
        p2_c2.accepted_ballot = AcceptedBallot::default();
        p2_c2.deps = HashSet::new();
        p2_c2.phase = TxnPhase::PreAccepted;
        // max_ballot_seen stays at 2 from the Recover
    }

    // Step 8: p2 sends Recover(ballot=3, c2) to {p1, p2}.
    let ballot_3 = BallotNumber(3);
    cluster.send(TestMessage {
        src: 2,
        dst: 1,
        payload: TestMessagePayload::Recover {
            ballot: ballot_3,
            txn_id: c2,
            t0: c2.0,
        },
    });
    cluster.send(TestMessage {
        src: 2,
        dst: 2,
        payload: TestMessagePayload::Recover {
            ballot: ballot_3,
            txn_id: c2,
            t0: c2.0,
        },
    });

    // Step 9: Deliver Recover(ballot=3) at p1.
    // p1 has never seen c2 before. Fresh state created.
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 9: p1 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                0,
                "Step 9: p1.accepted_ballot=0 (never saw c2 before)",
            );
            assert!(
                state.deps.is_empty(),
                "Step 9: p1 has no deps for c2 (never saw c1 on this key)",
            );
        }
        other => panic!("Step 9: expected RecoverOK, got {:?}", other),
    }
    {
        let p1_c2 = cluster
            .replica(1)
            .txn_states
            .get(&c2)
            .expect("Step 9: p1 must have state for c2");
        assert_eq!(
            (p1_c2.max_ballot_seen.0).0,
            3,
            "Step 9: p1.max_ballot_seen must be 3",
        );
    }

    // Step 10: Deliver Recover(ballot=3) at p2.
    // Queue: [Recover(3)->p2, RecoverOK->p2]
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 10: p2 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                0,
                "Step 10: p2.accepted_ballot=0 (reset — didn't receive Accept from step 7)",
            );
        }
        other => panic!("Step 10: expected RecoverOK, got {:?}", other),
    }
    {
        let p2_c2 = cluster
            .replica(2)
            .txn_states
            .get(&c2)
            .expect("Step 10: p2 must have state for c2");
        assert_eq!(
            (p2_c2.max_ballot_seen.0).0,
            3,
            "Step 10: p2.max_ballot_seen must be 3",
        );
    }

    // Clear RecoverOK responses.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // =========================================================================
    // Step 11: p2 (recovery coordinator) collects RecoverOK from p1 and p2.
    //   Both have accepted_ballot=0. p1 never saw c1 on this key, so deps={}.
    //   p2 also has empty deps (after our reset).
    //   Recovery chooses deps={}.
    //   p2 sends Accept(ballot=3, c2, deps={}) to {p1, p2}.
    // =========================================================================
    // (This is the coordinator logic — we manually inject the Accept messages.)

    // Step 12: (Included in step 11 — p1's PreAcceptOK was already addressed
    //   by the Recover. The spec mentions delivering PreAcceptOK(c2) at p1
    //   but we handle this via Recover response.)

    // Step 13: Deliver Accept(ballot=3, deps={}) at p1 and p2.
    cluster.send(TestMessage {
        src: 2,
        dst: 1,
        payload: TestMessagePayload::Accept {
            ballot: ballot_3,
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: Vec::new(), // deps={}
        },
    });
    cluster.send(TestMessage {
        src: 2,
        dst: 2,
        payload: TestMessagePayload::Accept {
            ballot: ballot_3,
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: Vec::new(), // deps={}
        },
    });

    // Deliver Accept(ballot=3) at p1.
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 13a: p1 sends AcceptOK");
    {
        let p1_c2 = cluster
            .replica(1)
            .txn_states
            .get(&c2)
            .expect("Step 13: p1 must have state for c2");
        assert_eq!(
            (p1_c2.accepted_ballot.0).0,
            3,
            "Step 13: p1.accepted_ballot must be 3 after Accept(ballot=3)",
        );
        assert!(
            p1_c2.deps.is_empty(),
            "Step 13: p1's deps must be empty after Accept(ballot=3, deps=empty)",
        );
        assert_eq!(p1_c2.phase, TxnPhase::Accepted);
    }

    // Deliver Accept(ballot=3) at p2.
    // Queue: [Accept(3)->p2, AcceptOK->p2]
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 13b: p2 sends AcceptOK");
    {
        let p2_c2 = cluster
            .replica(2)
            .txn_states
            .get(&c2)
            .expect("Step 13: p2 must have state for c2");
        assert_eq!(
            (p2_c2.accepted_ballot.0).0,
            3,
            "Step 13: p2.accepted_ballot must be 3 after Accept(ballot=3)",
        );
        assert!(
            p2_c2.deps.is_empty(),
            "Step 13: p2's deps must be empty after Accept(ballot=3, deps=empty)",
        );
    }

    // Step 14: AcceptOK responses (already generated; clear them).
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // =========================================================================
    // Steps 15-16: p1 initiates THIRD recovery for c2 with ballot=4.
    //   p1 sends Recover(ballot=4) to {p1, p3}.
    //
    //   Step 16 is THE CRITICAL STEP.
    // =========================================================================
    let ballot_4 = BallotNumber(4);
    cluster.send(TestMessage {
        src: 1,
        dst: 1,
        payload: TestMessagePayload::Recover {
            ballot: ballot_4,
            txn_id: c2,
            t0: c2.0,
        },
    });
    cluster.send(TestMessage {
        src: 1,
        dst: 3,
        payload: TestMessagePayload::Recover {
            ballot: ballot_4,
            txn_id: c2,
            t0: c2.0,
        },
    });

    // Deliver Recover(ballot=4) at p1.
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 15: p1 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                3,
                "Step 15: p1.accepted_ballot=3 (from step 13)",
            );
        }
        other => panic!("Step 15: expected RecoverOK, got {:?}", other),
    }

    // Step 16: Deliver Recover(ballot=4) at p3.
    // *** THE CRITICAL STEP ***
    // p3 updates max_ballot_seen=4 but accepted_ballot MUST stay at 2.
    // Queue: [Recover(4)->p3, RecoverOK->p1]
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 16: p3 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                2,
                "CRITICAL Step 16: p3.accepted_ballot must be 2 (from step 7), \
                 NOT corrupted by Recover's join_ballot. \
                 This is the EPaxos single-ballot-variable bug.",
            );
            assert!(
                state.deps.contains(&c1),
                "Step 16: p3's RecoverOK must report deps containing c1 (from step 7 Accept)",
            );
        }
        other => panic!("Step 16: expected RecoverOK, got {:?}", other),
    }
    {
        let p3_c2 = cluster
            .replica(3)
            .txn_states
            .get(&c2)
            .expect("Step 16: p3 must have state for c2");
        assert_eq!(
            (p3_c2.max_ballot_seen.0).0,
            4,
            "Step 16: p3.max_ballot_seen must be 4 after Recover(ballot=4)",
        );
        assert_eq!(
            (p3_c2.accepted_ballot.0).0,
            2,
            "CRITICAL Step 16: p3.accepted_ballot must STILL be 2. \
             Recover updates max_ballot_seen only, not accepted_ballot.",
        );
    }

    // Clear RecoverOK responses.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // =========================================================================
    // Steps 17-20: p1 initiates FOURTH recovery for c2 with ballot=5.
    //   p1 sends Recover(ballot=5) to {p1, p3}.
    // =========================================================================
    let ballot_5 = BallotNumber(5);
    cluster.send(TestMessage {
        src: 1,
        dst: 3,
        payload: TestMessagePayload::Recover {
            ballot: ballot_5,
            txn_id: c2,
            t0: c2.0,
        },
    });
    cluster.send(TestMessage {
        src: 1,
        dst: 1,
        payload: TestMessagePayload::Recover {
            ballot: ballot_5,
            txn_id: c2,
            t0: c2.0,
        },
    });

    // Step 18: Deliver Recover(ballot=5) at p3.
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 18: p3 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                2,
                "Step 18: p3.accepted_ballot still 2 after Recover(ballot=5)",
            );
        }
        other => panic!("Step 18: expected RecoverOK, got {:?}", other),
    }
    {
        let p3_c2 = cluster
            .replica(3)
            .txn_states
            .get(&c2)
            .expect("Step 18: p3 must have state for c2");
        assert_eq!(
            (p3_c2.max_ballot_seen.0).0,
            5,
            "Step 18: p3.max_ballot_seen must be 5",
        );
        assert_eq!(
            (p3_c2.accepted_ballot.0).0,
            2,
            "Step 18: p3.accepted_ballot must still be 2",
        );
    }

    // Step 19: Deliver Recover(ballot=5) at p1.
    // Queue: [Recover(5)->p1, RecoverOK->p1]
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 19: p1 sends RecoverOK");
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                3,
                "Step 19: p1.accepted_ballot=3 (from step 13)",
            );
            assert!(
                state.deps.is_empty(),
                "Step 19: p1's deps for c2 are empty (from step 13)",
            );
        }
        other => panic!("Step 19: expected RecoverOK, got {:?}", other),
    }
    {
        let p1_c2 = cluster
            .replica(1)
            .txn_states
            .get(&c2)
            .expect("Step 19: p1 must have state for c2");
        assert_eq!(
            (p1_c2.max_ballot_seen.0).0,
            5,
            "Step 19: p1.max_ballot_seen must be 5",
        );
        assert_eq!(
            (p1_c2.accepted_ballot.0).0,
            3,
            "Step 19: p1.accepted_ballot must be 3",
        );
    }

    // Step 20: Deliver duplicate Recover(ballot=5) at p1 — idempotent.
    // Clear existing responses first.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }
    cluster.send(TestMessage {
        src: 1,
        dst: 1,
        payload: TestMessagePayload::Recover {
            ballot: ballot_5,
            txn_id: c2,
            t0: c2.0,
        },
    });
    let responses = cluster.deliver_next();
    assert_eq!(
        responses.len(),
        1,
        "Step 20: duplicate Recover produces RecoverOK"
    );
    match &responses[0].payload {
        TestMessagePayload::RecoverOK { state, .. } => {
            assert_eq!(
                (state.accepted_ballot.0).0,
                3,
                "Step 20: idempotent — p1.accepted_ballot still 3",
            );
            assert_eq!(
                (state.max_ballot_seen.0).0,
                5,
                "Step 20: idempotent — p1.max_ballot_seen still 5",
            );
        }
        other => panic!("Step 20: expected RecoverOK, got {:?}", other),
    }

    // =========================================================================
    // Step 21: Recovery coordinator (p1) finalizes.
    //   Collected RecoverOK from p3 (accepted_ballot=2, deps={c1})
    //                       and p1 (accepted_ballot=3, deps={}).
    //
    //   Selection: max(accepted_ballot) = 3 (p1). Pick p1's value: deps={}.
    //
    //   WITH THE BUG: p3.accepted_ballot would be 4 (corrupted by
    //     join_ballot in step 16). Then max=4 > 3, pick p3's value:
    //     deps={c1}. WRONG — diverges from p2's committed deps={}.
    //
    //   WITH THE FIX: p3.accepted_ballot=2 < p1.accepted_ballot=3.
    //     Pick p1's value: deps={}. Correct — matches p2.
    // =========================================================================

    // Verify the selection logic.
    let p1_accepted = {
        let s = cluster.replica(1).txn_states.get(&c2).unwrap();
        ((s.accepted_ballot.0).0, s.deps.clone())
    };
    let p3_accepted = {
        let s = cluster.replica(3).txn_states.get(&c2).unwrap();
        ((s.accepted_ballot.0).0, s.deps.clone())
    };

    eprintln!("Step 21 — Recovery selection:");
    eprintln!(
        "  p1: accepted_ballot={}, deps={:?}",
        p1_accepted.0, p1_accepted.1
    );
    eprintln!(
        "  p3: accepted_ballot={}, deps={:?}",
        p3_accepted.0, p3_accepted.1
    );

    // The recovery coordinator picks the value from the replica with the
    // highest accepted_ballot.
    let (selected_deps, selected_from) = if p1_accepted.0 >= p3_accepted.0 {
        (p1_accepted.1.clone(), "p1")
    } else {
        (p3_accepted.1.clone(), "p3")
    };

    assert_eq!(
        p1_accepted.0, 3,
        "Step 21: p1.accepted_ballot must be 3 (from step 13)",
    );
    assert_eq!(
        p3_accepted.0, 2,
        "Step 21: p3.accepted_ballot must be 2 (from step 7, not corrupted)",
    );
    assert!(
        p1_accepted.0 > p3_accepted.0,
        "Step 21: p1's accepted_ballot (3) must be > p3's (2). \
         Recovery selects p1's value.",
    );
    assert_eq!(
        selected_from, "p1",
        "Step 21: recovery must select p1's value (highest accepted_ballot)",
    );
    assert!(
        selected_deps.is_empty(),
        "Step 21: selected deps must be empty (p1's value from step 13)",
    );

    // Clear pending.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // =========================================================================
    // Step 22: Recovery coordinator sends Accept(ballot=5, deps={}) to p3.
    //   p3 accepts.
    // =========================================================================
    cluster.send(TestMessage {
        src: 1,
        dst: 3,
        payload: TestMessagePayload::Accept {
            ballot: ballot_5,
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: Vec::new(), // selected deps={}
        },
    });
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Step 22: p3 sends AcceptOK");
    {
        let p3_c2 = cluster
            .replica(3)
            .txn_states
            .get(&c2)
            .expect("Step 22: p3 must have state for c2");
        assert_eq!(
            (p3_c2.accepted_ballot.0).0,
            5,
            "Step 22: p3.accepted_ballot updated to 5 after Accept(ballot=5)",
        );
        assert!(
            p3_c2.deps.is_empty(),
            "Step 22: p3's deps now empty after Accept(ballot=5, deps=empty)",
        );
    }

    // Clear pending.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // =========================================================================
    // Step 23: p2 commits c2 with deps={} (from step 13's Accept).
    // =========================================================================
    cluster.send(TestMessage {
        src: 2,
        dst: 2,
        payload: TestMessagePayload::Commit {
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: Vec::new(),
        },
    });
    cluster.deliver_next();
    {
        let p2_c2 = cluster
            .replica(2)
            .txn_states
            .get(&c2)
            .expect("Step 23: p2 must have state for c2");
        assert_eq!(p2_c2.phase, TxnPhase::Committed);
        assert!(p2_c2.deps.is_empty(), "Step 23: p2 commits with empty deps",);
    }

    // =========================================================================
    // Step 24: p1 commits c2 — must also use deps={}.
    //   With the fix: deps={} (matches p2). Correct.
    //   With the bug: deps={c1} (diverges from p2). LINEARIZABILITY VIOLATION.
    // =========================================================================
    cluster.send(TestMessage {
        src: 1,
        dst: 1,
        payload: TestMessagePayload::Commit {
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: selected_deps.iter().copied().collect(),
        },
    });
    cluster.deliver_next();
    {
        let p1_c2 = cluster
            .replica(1)
            .txn_states
            .get(&c2)
            .expect("Step 24: p1 must have state for c2");
        assert_eq!(p1_c2.phase, TxnPhase::Committed);
        assert!(
            p1_c2.deps.is_empty(),
            "Step 24: p1 commits with empty deps (consistent with p2)",
        );
    }

    // Also commit on p3 for completeness.
    cluster.send(TestMessage {
        src: 1,
        dst: 3,
        payload: TestMessagePayload::Commit {
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: selected_deps.iter().copied().collect(),
        },
    });
    cluster.deliver_next();

    // =========================================================================
    // FINAL ASSERTIONS
    // =========================================================================

    // PRIMARY: all replicas that committed must agree on deps.
    eprintln!("\n=== FINAL STATE ===");
    dump_ballot_state(&cluster, &c2);

    // Use the cluster's built-in consistency check.
    cluster.assert_consistent(&c2);

    // Explicit check: p1 and p2 agree on committed deps for c2.
    let p1_committed_deps = &cluster.replica(1).txn_states.get(&c2).unwrap().deps;
    let p2_committed_deps = &cluster.replica(2).txn_states.get(&c2).unwrap().deps;
    let p3_committed_deps = &cluster.replica(3).txn_states.get(&c2).unwrap().deps;

    assert_eq!(
        p1_committed_deps, p2_committed_deps,
        "LINEARIZABILITY: p1 and p2 must agree on committed deps for c2. \
         p1={:?}, p2={:?}",
        p1_committed_deps, p2_committed_deps,
    );
    assert_eq!(
        p2_committed_deps, p3_committed_deps,
        "LINEARIZABILITY: p2 and p3 must agree on committed deps for c2. \
         p2={:?}, p3={:?}",
        p2_committed_deps, p3_committed_deps,
    );

    // All committed with deps={}.
    assert!(
        p1_committed_deps.is_empty(),
        "All replicas must commit c2 with empty deps",
    );

    // SECONDARY: verify the critical ballot separation held throughout.
    // p3's accepted_ballot was 2 at step 16, never corrupted to 4.
    // (Already verified at step 16, but re-check final state.)
    let p3_final = cluster.replica(3).txn_states.get(&c2).unwrap();
    assert_eq!(
        (p3_final.accepted_ballot.0).0,
        5,
        "p3's final accepted_ballot is 5 (from step 22 Accept)",
    );
    assert_eq!(
        (p3_final.max_ballot_seen.0).0,
        5,
        "p3's final max_ballot_seen is 5",
    );
}

/// Verify that deps in the Accept message payload use Vec but the
/// replica converts to HashSet internally.
#[test]
fn epaxos_deps_hashset_consistency() {
    let mut cluster = TestCluster::new(3);
    let key = b"test_key".to_vec();
    let c1 = make_txn_id(1, 100);
    let c2 = make_txn_id(2, 200);

    // Pre-accept c1 at p1 so it shows up in conflict index.
    // Use p2 as src so the PreAcceptOK response goes to p2 (not p1).
    cluster.send(TestMessage {
        src: 2,
        dst: 1,
        payload: TestMessagePayload::PreAccept {
            txn_id: c1,
            t0: c1.0,
            key: key.clone(),
        },
    });
    let responses = cluster.deliver_next();
    assert_eq!(
        responses.len(),
        1,
        "PreAccept(c1) should produce one PreAcceptOK"
    );

    // Drop the enqueued PreAcceptOK response — we don't need it.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // Pre-accept c2 at p1 — should see c1 as dependency.
    cluster.send(TestMessage {
        src: 2,
        dst: 1,
        payload: TestMessagePayload::PreAccept {
            txn_id: c2,
            t0: c2.0,
            key: key.clone(),
        },
    });
    let responses = cluster.deliver_next();
    assert_eq!(
        responses.len(),
        1,
        "PreAccept(c2) should produce one PreAcceptOK"
    );
    match &responses[0].payload {
        TestMessagePayload::PreAcceptOK { deps, .. } => {
            assert!(deps.contains(&c1), "c2 deps should contain c1");
        }
        other => panic!("expected PreAcceptOK, got {:?}", other),
    }

    // Drop enqueued response.
    while cluster.pending_count() > 0 {
        cluster.drop_at(0);
    }

    // Accept with deps containing c1 (as Vec).
    let ballot = BallotNumber(1);
    cluster.send(TestMessage {
        src: 2,
        dst: 1,
        payload: TestMessagePayload::Accept {
            ballot,
            txn_id: c2,
            t0: c2.0,
            t: c2.0,
            deps: vec![c1],
        },
    });
    let responses = cluster.deliver_next();
    assert_eq!(responses.len(), 1, "Accept should produce one AcceptOK");

    // Verify internal state uses HashSet.
    let state = cluster.replica(1).txn_states.get(&c2).unwrap();
    assert!(state.deps.contains(&c1));
    assert_eq!(state.deps.len(), 1);
}
