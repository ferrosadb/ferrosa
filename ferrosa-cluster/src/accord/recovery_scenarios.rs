//! Failure and recovery scenario tests for the Accord protocol (A3.9).
//!
//! These tests exercise coordinator crashes at various protocol phases and
//! verify that the recovery protocol correctly restores consensus. All tests
//! use the deterministic [`TestCluster`] harness — no tokio, no timers,
//! fully reproducible.

#[cfg(test)]
mod tests {
    use crate::accord::recovery::{RecoverOKResponse, RecoveryCoordinator, RecoveryDecision};
    use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};
    use ferrosa_common::accord::{
        AcceptedBallot, BallotGenerator, BallotNumber, PromisedBallot, Timestamp, TxnId, TxnPhase,
        TxnState,
    };
    use std::collections::HashSet;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
    }

    /// Build a TxnState with a specific accepted ballot and values.
    fn state_with_accepted(
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: HashSet<TxnId>,
        accepted_ballot: BallotNumber,
    ) -> TxnState {
        let mut state = TxnState::new(txn_id, t0);
        state.t = t;
        state.deps = deps;
        state.accepted_ballot = AcceptedBallot(accepted_ballot);
        state.max_ballot_seen = PromisedBallot(accepted_ballot);
        if accepted_ballot != BallotNumber::default() {
            state.phase = TxnPhase::Accepted;
        }
        state
    }

    /// Build a RecoverOKResponse from a TxnState.
    fn recover_ok(from: u64, state: TxnState) -> RecoverOKResponse {
        RecoverOKResponse {
            from,
            state,
            superseding: vec![],
            waiting: vec![],
        }
    }

    /// Run a full PreAccept round on all replicas in a 5-node cluster.
    /// Coordinator is node 1, sends PreAccept to replicas 2..=5.
    fn preaccept_on_cluster(cluster: &mut TestCluster, txn_id: TxnId, t0: Timestamp, key: &[u8]) {
        for dst in 2..=5u64 {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id,
                    t0,
                    key: key.to_vec(),
                },
            });
        }
        // Deliver all PreAccept messages (4 messages, producing 4 PreAcceptOK responses).
        for _ in 0..4 {
            cluster.deliver_next();
        }
    }

    /// Send Accept to replicas and deliver all messages until quiescent.
    /// Returns number of AcceptOK responses observed.
    fn accept_on_replicas(
        cluster: &mut TestCluster,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        ballot: BallotNumber,
        targets: &[u64],
    ) -> usize {
        // Drain any pending messages first (e.g., PreAcceptOK responses).
        while cluster.pending_count() > 0 {
            cluster.deliver_next();
        }

        for &dst in targets {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::Accept {
                    ballot,
                    txn_id,
                    t0,
                    t,
                    deps: vec![],
                },
            });
        }

        // Deliver all Accept messages and their AcceptOK responses.
        let mut accept_ok_count = 0;
        while cluster.pending_count() > 0 {
            let responses = cluster.deliver_next();
            for r in &responses {
                if matches!(r.payload, TestMessagePayload::AcceptOK { .. }) {
                    accept_ok_count += 1;
                }
            }
        }
        accept_ok_count
    }

    /// Send Commit to replicas and drain all messages.
    fn commit_on_replicas(
        cluster: &mut TestCluster,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        targets: &[u64],
    ) {
        // Drain pending messages first.
        while cluster.pending_count() > 0 {
            cluster.deliver_next();
        }

        for &dst in targets {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::Commit {
                    txn_id,
                    t0,
                    t,
                    deps: vec![],
                },
            });
        }
        // Commit generates no responses, but drain to be safe.
        while cluster.pending_count() > 0 {
            cluster.deliver_next();
        }
    }

    /// Send Recover to a set of replicas and collect all response messages.
    /// Drains the queue first, then delivers all Recover messages and their
    /// responses until quiescent.
    fn recover_from_replicas(
        cluster: &mut TestCluster,
        recovery_node: u64,
        txn_id: TxnId,
        t0: Timestamp,
        ballot: BallotNumber,
        targets: &[u64],
    ) -> Vec<TestMessage> {
        // Drain any pending messages first.
        while cluster.pending_count() > 0 {
            cluster.deliver_next();
        }

        for &dst in targets {
            cluster.send(TestMessage {
                src: recovery_node,
                dst,
                payload: TestMessagePayload::Recover { ballot, txn_id, t0 },
            });
        }

        // Deliver all Recover messages and collect responses.
        let mut responses = Vec::new();
        while cluster.pending_count() > 0 {
            let msgs = cluster.deliver_next();
            responses.extend(msgs);
        }
        responses
    }

    // =======================================================================
    // A3.9 — Failure and Recovery Scenarios (11 tests)
    // =======================================================================

    // -----------------------------------------------------------------------
    // 1. scenario_coordinator_crash_after_preaccept
    // -----------------------------------------------------------------------

    /// Coordinator sends PreAccept to all replicas, then crashes before
    /// Accept. A recovery coordinator picks up and drives the transaction
    /// to completion. Replicas should agree on the committed state.
    #[test]
    fn scenario_coordinator_crash_after_preaccept() {
        let mut cluster = TestCluster::new(5);
        let t0 = ts(1000);
        let txn_id = txn(1, 1000);

        // Phase 1: Coordinator (node 1) sends PreAccept to replicas 2-5.
        preaccept_on_cluster(&mut cluster, txn_id, t0, b"key1");

        // Drain PreAcceptOK responses (coordinator "crashes" before processing them).
        while cluster.pending_count() > 0 {
            cluster.deliver_next();
        }

        // Phase 2: Node 3 initiates recovery with a fresh ballot.
        let recovery_ballot = BallotNumber(10);
        let responses =
            recover_from_replicas(&mut cluster, 3, txn_id, t0, recovery_ballot, &[2, 4, 5]);

        // All three replicas should respond with RecoverOK.
        let recover_oks: Vec<_> = responses
            .iter()
            .filter(|m| matches!(m.payload, TestMessagePayload::RecoverOK { .. }))
            .collect();
        assert_eq!(
            recover_oks.len(),
            3,
            "expected 3 RecoverOK responses, got {}",
            recover_oks.len()
        );

        // Phase 3: Feed responses into RecoveryCoordinator.
        let ballot_gen = BallotGenerator::new();
        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &ballot_gen);

        let mut decision = None;
        for msg in &recover_oks {
            if let TestMessagePayload::RecoverOK {
                txn_id: _,
                state,
                superseding,
                wait,
            } = &msg.payload
            {
                let resp = RecoverOKResponse {
                    from: msg.src,
                    state: state.clone(),
                    superseding: superseding.clone(),
                    waiting: wait.clone(),
                };
                if let Some(d) = coord.handle_recover_ok(resp) {
                    decision = Some(d);
                }
            }
        }

        // No replica has accepted anything, so recovery should decide RunPreAccept.
        assert!(decision.is_some(), "expected a recovery decision");
        assert_eq!(
            decision.unwrap(),
            RecoveryDecision::RunPreAccept,
            "after crash post-PreAccept (no Accept), recovery should run PreAccept from scratch"
        );
    }

    // -----------------------------------------------------------------------
    // 2. scenario_coordinator_crash_after_accept
    // -----------------------------------------------------------------------

    /// Coordinator sends Accept to a subset of replicas, then crashes before
    /// reaching a quorum on Commit. Recovery should find the accepted value
    /// and commit it.
    #[test]
    fn scenario_coordinator_crash_after_accept() {
        let mut cluster = TestCluster::new(5);
        let t0 = ts(2000);
        let txn_id = txn(1, 2000);
        let t = ts(2001);

        // Phase 1: PreAccept on all replicas.
        preaccept_on_cluster(&mut cluster, txn_id, t0, b"key2");

        // Phase 2: Accept reaches replicas 2 and 3 only, then coordinator crashes.
        let ballot = BallotNumber(1);
        let accept_count = accept_on_replicas(&mut cluster, txn_id, t0, t, ballot, &[2, 3]);
        assert_eq!(accept_count, 2, "two replicas should have accepted");

        // Phase 3: Node 4 initiates recovery.
        let recovery_ballot = BallotNumber(20);
        let responses =
            recover_from_replicas(&mut cluster, 4, txn_id, t0, recovery_ballot, &[2, 3, 5]);

        // Feed into RecoveryCoordinator.
        let ballot_gen = BallotGenerator::new();
        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &ballot_gen);

        let mut decision = None;
        for msg in &responses {
            if let TestMessagePayload::RecoverOK {
                state,
                superseding,
                wait,
                ..
            } = &msg.payload
            {
                let resp = RecoverOKResponse {
                    from: msg.src,
                    state: state.clone(),
                    superseding: superseding.clone(),
                    waiting: wait.clone(),
                };
                if let Some(d) = coord.handle_recover_ok(resp) {
                    decision = Some(d);
                }
            }
        }

        // Replicas 2 and 3 accepted at ballot 1, replica 5 has no accepted state.
        // Recovery should commit with the accepted value (t=2001).
        match decision.expect("expected a recovery decision") {
            RecoveryDecision::Commit { timestamp, .. } => {
                assert_eq!(
                    timestamp, t,
                    "recovery should commit with the accepted timestamp"
                );
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 3. scenario_coordinator_crash_after_commit
    // -----------------------------------------------------------------------

    /// Coordinator commits on a subset of replicas, then crashes. Recovery
    /// finds committed state and re-commits.
    #[test]
    fn scenario_coordinator_crash_after_commit() {
        let mut cluster = TestCluster::new(5);
        let t0 = ts(3000);
        let txn_id = txn(1, 3000);
        let t = ts(3001);

        // Phase 1 + 2: PreAccept and Accept on all replicas.
        preaccept_on_cluster(&mut cluster, txn_id, t0, b"key3");
        let ballot = BallotNumber(1);
        accept_on_replicas(&mut cluster, txn_id, t0, t, ballot, &[2, 3, 4, 5]);

        // Phase 3: Commit only reaches replica 2.
        commit_on_replicas(&mut cluster, txn_id, t0, t, &[2]);

        // Phase 4: Node 5 initiates recovery, asking replicas 2, 3, 4.
        let recovery_ballot = BallotNumber(30);
        let responses =
            recover_from_replicas(&mut cluster, 5, txn_id, t0, recovery_ballot, &[2, 3, 4]);

        // Feed into RecoveryCoordinator.
        let ballot_gen = BallotGenerator::new();
        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &ballot_gen);

        let mut decision = None;
        for msg in &responses {
            if let TestMessagePayload::RecoverOK {
                state,
                superseding,
                wait,
                ..
            } = &msg.payload
            {
                let resp = RecoverOKResponse {
                    from: msg.src,
                    state: state.clone(),
                    superseding: superseding.clone(),
                    waiting: wait.clone(),
                };
                if let Some(d) = coord.handle_recover_ok(resp) {
                    decision = Some(d);
                }
            }
        }

        // Replica 2 is committed, so recovery should commit with same values.
        match decision.expect("expected a recovery decision") {
            RecoveryDecision::Commit { timestamp, .. } => {
                assert_eq!(
                    timestamp, t,
                    "recovery should commit with the already-committed timestamp"
                );
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 4. scenario_recovery_with_no_accepted_state
    // -----------------------------------------------------------------------

    /// All replicas report no accepted state for a transaction.
    /// Recovery should decide RunPreAccept.
    #[test]
    fn scenario_recovery_with_no_accepted_state() {
        let ballot_gen = BallotGenerator::new();
        let t0 = ts(4000);
        let txn_id = txn(1, 4000);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &ballot_gen);

        // Three replicas respond with unseen (default) state — quorum of 5.
        for from in 1..=3u64 {
            let state = TxnState::new(txn_id, t0);
            let d = coord.handle_recover_ok(recover_ok(from, state));
            if from < 3 {
                assert!(d.is_none(), "no decision before quorum");
            } else {
                match d.expect("expected decision at quorum") {
                    RecoveryDecision::RunPreAccept => {} // correct
                    other => panic!("expected RunPreAccept, got {:?}", other),
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // 5. scenario_recovery_with_superseding_txn
    // -----------------------------------------------------------------------

    /// Replicas report superseding transactions during recovery.
    /// The recovery coordinator should collect them.
    #[test]
    fn scenario_recovery_with_superseding_txn() {
        let ballot_gen = BallotGenerator::new();
        let t0 = ts(5000);
        let txn_id = txn(1, 5000);

        let superseding_txn = txn(2, 6000);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        // Replica 1: no accepted state, but reports a superseding txn.
        let state1 = TxnState::new(txn_id, t0);
        let mut resp1 = recover_ok(1, state1);
        resp1.superseding = vec![superseding_txn];
        coord.handle_recover_ok(resp1);

        // Replica 2: also reports the same superseding txn (dedup test).
        let state2 = TxnState::new(txn_id, t0);
        let mut resp2 = recover_ok(2, state2);
        resp2.superseding = vec![superseding_txn];
        let decision = coord.handle_recover_ok(resp2);

        // Should have RunPreAccept decision (no accepted state).
        assert_eq!(
            decision.unwrap(),
            RecoveryDecision::RunPreAccept,
            "no accepted state => RunPreAccept"
        );

        // Superseding set should have exactly one (deduplicated).
        let superseding = coord.superseding_txns();
        assert_eq!(superseding.len(), 1);
        assert_eq!(superseding[0], superseding_txn);
    }

    // -----------------------------------------------------------------------
    // 6. scenario_recovery_with_wait_set
    // -----------------------------------------------------------------------

    /// Replicas report waiting transactions during recovery.
    /// The recovery coordinator collects and deduplicates them.
    #[test]
    fn scenario_recovery_with_wait_set() {
        let ballot_gen = BallotGenerator::new();
        let t0 = ts(6000);
        let txn_id = txn(1, 6000);

        let waiting_a = txn(3, 7000);
        let waiting_b = txn(4, 8000);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        // Replica 1: reports waiting_a.
        let mut resp1 = recover_ok(1, TxnState::new(txn_id, t0));
        resp1.waiting = vec![waiting_a];
        coord.handle_recover_ok(resp1);

        // Replica 2: reports waiting_a (dup) and waiting_b.
        let mut resp2 = recover_ok(2, TxnState::new(txn_id, t0));
        resp2.waiting = vec![waiting_a, waiting_b];
        coord.handle_recover_ok(resp2);

        let waiting = coord.waiting_txns();
        assert_eq!(waiting.len(), 2, "should deduplicate waiting txns");
        assert!(waiting.contains(&waiting_a));
        assert!(waiting.contains(&waiting_b));
    }

    // -----------------------------------------------------------------------
    // 7. scenario_two_recoveries_same_txn
    // -----------------------------------------------------------------------

    /// Two nodes simultaneously attempt recovery for the same transaction.
    /// Both should independently arrive at the same decision (determinism).
    #[test]
    fn scenario_two_recoveries_same_txn() {
        let mut cluster = TestCluster::new(5);
        let t0 = ts(7000);
        let txn_id = txn(1, 7000);
        let t = ts(7001);

        // PreAccept + Accept on replicas 2, 3, 4.
        preaccept_on_cluster(&mut cluster, txn_id, t0, b"key7");
        let ballot = BallotNumber(1);
        accept_on_replicas(&mut cluster, txn_id, t0, t, ballot, &[2, 3, 4]);

        // Recovery A: node 3 asks replicas 2, 4, 5.
        let ballot_a = BallotNumber(50);
        let responses_a = recover_from_replicas(&mut cluster, 3, txn_id, t0, ballot_a, &[2, 4, 5]);

        // Recovery B: node 5 asks replicas 2, 3, 4 with a higher ballot.
        let ballot_b = BallotNumber(60);
        let responses_b = recover_from_replicas(&mut cluster, 5, txn_id, t0, ballot_b, &[2, 3, 4]);

        // Process Recovery A.
        let gen_a = BallotGenerator::new();
        let mut coord_a = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &gen_a);
        let mut decision_a = None;
        for msg in &responses_a {
            if let TestMessagePayload::RecoverOK {
                state,
                superseding,
                wait,
                ..
            } = &msg.payload
            {
                if let Some(d) = coord_a.handle_recover_ok(RecoverOKResponse {
                    from: msg.src,
                    state: state.clone(),
                    superseding: superseding.clone(),
                    waiting: wait.clone(),
                }) {
                    decision_a = Some(d);
                }
            }
        }

        // Process Recovery B.
        let gen_b = BallotGenerator::new();
        let mut coord_b = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &gen_b);
        let mut decision_b = None;
        for msg in &responses_b {
            if let TestMessagePayload::RecoverOK {
                state,
                superseding,
                wait,
                ..
            } = &msg.payload
            {
                if let Some(d) = coord_b.handle_recover_ok(RecoverOKResponse {
                    from: msg.src,
                    state: state.clone(),
                    superseding: superseding.clone(),
                    waiting: wait.clone(),
                }) {
                    decision_b = Some(d);
                }
            }
        }

        // Both recoveries must agree on the same outcome.
        let da = decision_a.expect("recovery A must decide");
        let db = decision_b.expect("recovery B must decide");

        // Both should commit with the accepted value.
        match (&da, &db) {
            (
                RecoveryDecision::Commit {
                    timestamp: ta,
                    deps: deps_a,
                },
                RecoveryDecision::Commit {
                    timestamp: tb,
                    deps: deps_b,
                },
            ) => {
                assert_eq!(
                    ta, tb,
                    "two recoveries of the same txn must select the same timestamp"
                );
                assert_eq!(
                    deps_a, deps_b,
                    "two recoveries of the same txn must select the same deps"
                );
            }
            _ => panic!("expected both Commit, got A={:?}, B={:?}", da, db),
        }
    }

    // -----------------------------------------------------------------------
    // 8. scenario_recover_after_accept_different_ballot
    // -----------------------------------------------------------------------

    /// Replicas accepted at different ballots. Recovery must select the
    /// value from the highest accepted ballot.
    #[test]
    fn scenario_recover_after_accept_different_ballot() {
        let ballot_gen = BallotGenerator::new();
        let t0 = ts(8000);
        let txn_id = txn(1, 8000);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &ballot_gen);

        // Replica 1: accepted at ballot 3, timestamp 8001.
        let state1 = state_with_accepted(txn_id, t0, ts(8001), HashSet::new(), BallotNumber(3));
        coord.handle_recover_ok(recover_ok(1, state1));

        // Replica 2: accepted at ballot 7, timestamp 8005.
        let state2 = state_with_accepted(txn_id, t0, ts(8005), HashSet::new(), BallotNumber(7));
        coord.handle_recover_ok(recover_ok(2, state2));

        // Replica 3: accepted at ballot 5, timestamp 8003.
        let state3 = state_with_accepted(txn_id, t0, ts(8003), HashSet::new(), BallotNumber(5));
        let decision = coord
            .handle_recover_ok(recover_ok(3, state3))
            .expect("expected decision at quorum");

        // Must select ballot 7's value (timestamp 8005).
        match decision {
            RecoveryDecision::Commit { timestamp, .. } => {
                assert_eq!(
                    timestamp,
                    ts(8005),
                    "recovery must select the highest accepted ballot's value"
                );
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 9. scenario_recover_selects_highest_accepted_ballot
    // -----------------------------------------------------------------------

    /// Edge case: two replicas have accepted state, one has none. The one
    /// with the higher accepted_ballot wins, even if the other has a higher
    /// max_ballot_seen (promised).
    #[test]
    fn scenario_recover_selects_highest_accepted_ballot() {
        let ballot_gen = BallotGenerator::new();
        let t0 = ts(9000);
        let txn_id = txn(1, 9000);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        // Replica 1: accepted at ballot 10, timestamp 9010.
        let state1 = state_with_accepted(txn_id, t0, ts(9010), HashSet::new(), BallotNumber(10));
        coord.handle_recover_ok(recover_ok(1, state1));

        // Replica 2: accepted at ballot 5, timestamp 9005, but max_ballot_seen=20.
        let mut state2 = state_with_accepted(txn_id, t0, ts(9005), HashSet::new(), BallotNumber(5));
        state2.max_ballot_seen = PromisedBallot(BallotNumber(20));
        let decision = coord
            .handle_recover_ok(recover_ok(2, state2))
            .expect("expected decision at quorum");

        // Must select ballot 10's value, NOT ballot 5's (despite its higher promised).
        match decision {
            RecoveryDecision::Commit { timestamp, .. } => {
                assert_eq!(
                    timestamp,
                    ts(9010),
                    "recovery must select by accepted_ballot (10), not max_ballot_seen (20)"
                );
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 10. scenario_duplicate_recover_is_idempotent
    // -----------------------------------------------------------------------

    /// Sending the same Recover message twice to a replica produces the
    /// same response and does not corrupt state.
    #[test]
    fn scenario_duplicate_recover_is_idempotent() {
        let mut cluster = TestCluster::new(3);
        let t0 = ts(10000);
        let txn_id = txn(1, 10000);
        let t = ts(10001);

        // Set up: Accept on replica 2.
        cluster.send(TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::Accept {
                ballot: BallotNumber(1),
                txn_id,
                t0,
                t,
                deps: vec![],
            },
        });
        cluster.deliver_next(); // delivers Accept, enqueues AcceptOK
        cluster.deliver_next(); // delivers AcceptOK

        // First Recover.
        cluster.send(TestMessage {
            src: 3,
            dst: 2,
            payload: TestMessagePayload::Recover {
                ballot: BallotNumber(10),
                txn_id,
                t0,
            },
        });
        let first_responses = cluster.deliver_next();
        assert_eq!(first_responses.len(), 1);

        // Drain the RecoverOK so queue is clean.
        while cluster.pending_count() > 0 {
            cluster.deliver_next();
        }

        // Second (duplicate) Recover with same ballot.
        cluster.send(TestMessage {
            src: 3,
            dst: 2,
            payload: TestMessagePayload::Recover {
                ballot: BallotNumber(10),
                txn_id,
                t0,
            },
        });
        let second_responses = cluster.deliver_next();
        assert_eq!(second_responses.len(), 1);

        // Both responses must contain the same state.
        let extract_state = |responses: &[TestMessage]| -> TxnState {
            match &responses[0].payload {
                TestMessagePayload::RecoverOK { state, .. } => state.clone(),
                other => panic!("expected RecoverOK, got {:?}", other),
            }
        };

        let state1 = extract_state(&first_responses);
        let state2 = extract_state(&second_responses);

        assert_eq!(state1.phase, state2.phase, "phase must be identical");
        assert_eq!(state1.t, state2.t, "timestamp must be identical");
        assert_eq!(
            state1.accepted_ballot, state2.accepted_ballot,
            "accepted_ballot must be identical"
        );
        assert_eq!(state1.deps, state2.deps, "deps must be identical");
    }

    // -----------------------------------------------------------------------
    // 11. scenario_three_recoveries_escalating_ballots
    // -----------------------------------------------------------------------

    /// Three successive recovery attempts with escalating ballots.
    /// Each new recovery should successfully promise at a higher ballot,
    /// and the final recovery should still find the originally accepted value.
    #[test]
    fn scenario_three_recoveries_escalating_ballots() {
        let mut cluster = TestCluster::new(5);
        let t0 = ts(11000);
        let txn_id = txn(1, 11000);
        let t = ts(11001);

        // Accept on replicas 2, 3 at ballot 1.
        preaccept_on_cluster(&mut cluster, txn_id, t0, b"key11");
        accept_on_replicas(&mut cluster, txn_id, t0, t, BallotNumber(1), &[2, 3]);

        // Recovery 1: ballot 10, from node 4, asks replicas 2, 3, 5.
        let responses_1 =
            recover_from_replicas(&mut cluster, 4, txn_id, t0, BallotNumber(10), &[2, 3, 5]);
        let recover_ok_count_1 = responses_1
            .iter()
            .filter(|m| matches!(m.payload, TestMessagePayload::RecoverOK { .. }))
            .count();
        assert_eq!(
            recover_ok_count_1, 3,
            "first recovery should get 3 RecoverOK"
        );

        // Recovery 2: ballot 20, from node 5, asks replicas 2, 3, 4.
        let responses_2 =
            recover_from_replicas(&mut cluster, 5, txn_id, t0, BallotNumber(20), &[2, 3, 4]);
        let recover_ok_count_2 = responses_2
            .iter()
            .filter(|m| matches!(m.payload, TestMessagePayload::RecoverOK { .. }))
            .count();
        assert_eq!(
            recover_ok_count_2, 3,
            "second recovery should get 3 RecoverOK"
        );

        // Recovery 3: ballot 30, from node 3, asks replicas 2, 4, 5.
        let responses_3 =
            recover_from_replicas(&mut cluster, 3, txn_id, t0, BallotNumber(30), &[2, 4, 5]);
        let recover_ok_count_3 = responses_3
            .iter()
            .filter(|m| matches!(m.payload, TestMessagePayload::RecoverOK { .. }))
            .count();
        assert_eq!(
            recover_ok_count_3, 3,
            "third recovery should get 3 RecoverOK"
        );

        // Feed final recovery responses into coordinator.
        let gen = BallotGenerator::new();
        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &gen);
        let mut decision = None;
        for msg in &responses_3 {
            if let TestMessagePayload::RecoverOK {
                state,
                superseding,
                wait,
                ..
            } = &msg.payload
            {
                if let Some(d) = coord.handle_recover_ok(RecoverOKResponse {
                    from: msg.src,
                    state: state.clone(),
                    superseding: superseding.clone(),
                    waiting: wait.clone(),
                }) {
                    decision = Some(d);
                }
            }
        }

        // The accepted value (t=11001 at ballot 1) should still be recovered.
        match decision.expect("expected decision from third recovery") {
            RecoveryDecision::Commit { timestamp, .. } => {
                assert_eq!(
                    timestamp, t,
                    "third recovery must still find the originally accepted value"
                );
            }
            other => panic!("expected Commit, got {:?}", other),
        }

        // Verify ballot promises escalated on replica 2.
        let state = cluster.replica(2).txn_states.get(&txn_id).unwrap();
        assert_eq!(
            state.max_ballot_seen,
            PromisedBallot(BallotNumber(30)),
            "replica 2 should have promised ballot 30 after three recoveries"
        );
        // Accepted ballot should NOT have changed from the original Accept.
        assert_eq!(
            state.accepted_ballot,
            AcceptedBallot(BallotNumber(1)),
            "accepted_ballot must remain at the original Accept ballot"
        );
    }
}
