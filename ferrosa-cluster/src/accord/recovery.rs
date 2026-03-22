//! Recovery coordinator for the Accord consensus protocol.
//!
//! When a coordinator crashes or becomes unreachable during a transaction,
//! another node can initiate recovery. The [`RecoveryCoordinator`] generates
//! a fresh ballot, broadcasts `Recover` messages, collects `RecoverOK`
//! responses, and decides the outcome based on the **highest accepted_ballot**
//! (NOT `max_ballot_seen` — this distinction is critical for correctness).
//!
//! # Selection algorithm
//!
//! Responses are grouped by `accepted_ballot`. The value (timestamp, deps)
//! associated with the highest `accepted_ballot` wins. This matches the
//! standard Paxos recovery rule: pick the value from the highest-numbered
//! ballot in which a value was actually accepted.

use ferrosa_common::accord::{
    AcceptedBallot, BallotGenerator, BallotNumber, Timestamp, TxnId, TxnPhase, TxnState,
};
use std::collections::HashSet;

// ---------------------------------------------------------------------------
// RecoveryDecision
// ---------------------------------------------------------------------------

/// The outcome of a recovery attempt, decided once a quorum of
/// `RecoverOK` responses has been collected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryDecision {
    /// A value was accepted at some ballot — commit it with these deps and timestamp.
    Commit {
        deps: HashSet<TxnId>,
        timestamp: Timestamp,
    },
    /// No replica has seen this transaction — run the PreAccept phase from scratch.
    RunPreAccept,
    /// Transaction should be aborted (e.g., superseded by a conflicting transaction).
    Abort,
}

// ---------------------------------------------------------------------------
// RecoverOKResponse
// ---------------------------------------------------------------------------

/// A single `RecoverOK` response from a replica.
#[derive(Debug, Clone)]
pub struct RecoverOKResponse {
    /// The replica's node ID.
    pub from: u64,
    /// The replica's current state for this transaction.
    pub state: TxnState,
    /// Transactions that have superseded the recovered one on this replica.
    pub superseding: Vec<TxnId>,
    /// Transactions waiting on the recovered one on this replica.
    pub waiting: Vec<TxnId>,
}

// ---------------------------------------------------------------------------
// RecoveryCoordinator
// ---------------------------------------------------------------------------

/// Coordinates recovery of a single Accord transaction.
///
/// Usage:
/// 1. Call [`RecoveryCoordinator::start_recovery`] to create a coordinator
///    and get the ballot to use in `Recover` messages.
/// 2. For each `RecoverOK` received, call [`handle_recover_ok`].
/// 3. When enough responses are collected, the method returns a
///    [`RecoveryDecision`].
pub struct RecoveryCoordinator {
    /// The ballot number used for this recovery attempt.
    pub ballot_number: BallotNumber,
    /// The transaction being recovered.
    pub txn_id: TxnId,
    /// The original proposed timestamp (t0) for the transaction.
    pub t0: Timestamp,
    /// Total number of replicas in the electorate.
    pub cluster_size: usize,
    /// Collected RecoverOK responses.
    responses: Vec<RecoverOKResponse>,
}

impl RecoveryCoordinator {
    /// Start a recovery attempt for `txn_id`.
    ///
    /// Generates a fresh ballot from `ballot_gen` and returns the
    /// coordinator along with the ballot number to use in `Recover` messages.
    pub fn start_recovery(
        txn_id: TxnId,
        t0: Timestamp,
        cluster_size: usize,
        ballot_gen: &BallotGenerator,
    ) -> Self {
        assert!(cluster_size > 0, "cluster_size must be positive");
        let ballot_number = ballot_gen.fresh_ballot();
        Self {
            ballot_number,
            txn_id,
            t0,
            cluster_size,
            responses: Vec::new(),
        }
    }

    /// The quorum size: a strict majority of the cluster.
    fn quorum_size(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Process a `RecoverOK` response. Returns `Some(decision)` once a
    /// quorum has been reached, `None` if more responses are needed.
    ///
    /// # Selection rule
    ///
    /// Among all responses, the value from the response with the **highest
    /// `accepted_ballot`** is selected. This is the standard Paxos rule:
    /// we must re-propose the value that was accepted at the highest ballot,
    /// because that ballot's proposer may have achieved a quorum.
    ///
    /// If NO response has a non-zero `accepted_ballot` (i.e., no replica
    /// has accepted any value), we run PreAccept from scratch.
    pub fn handle_recover_ok(&mut self, response: RecoverOKResponse) -> Option<RecoveryDecision> {
        assert_eq!(
            response.state.txn_id, self.txn_id,
            "RecoverOK for wrong txn: expected {:?}, got {:?}",
            self.txn_id, response.state.txn_id,
        );

        self.responses.push(response);

        if self.responses.len() < self.quorum_size() {
            return None;
        }

        Some(self.decide())
    }

    /// Make a recovery decision based on the collected responses.
    ///
    /// CRITICAL: Selection is by max(accepted_ballot), NOT max(max_ballot_seen).
    fn decide(&self) -> RecoveryDecision {
        // Check if any response reports a committed transaction.
        for resp in &self.responses {
            if resp.state.phase == TxnPhase::Committed || resp.state.phase == TxnPhase::Applied {
                return RecoveryDecision::Commit {
                    deps: resp.state.deps.clone(),
                    timestamp: resp.state.t,
                };
            }
        }

        // Find the response with the highest accepted_ballot.
        let highest_accepted = self
            .responses
            .iter()
            .max_by_key(|r| r.state.accepted_ballot);

        match highest_accepted {
            Some(resp) if resp.state.accepted_ballot != AcceptedBallot::default() => {
                // A value was accepted at some ballot — re-propose it.
                RecoveryDecision::Commit {
                    deps: resp.state.deps.clone(),
                    timestamp: resp.state.t,
                }
            }
            _ => {
                // No replica has accepted any value for this txn.
                // Need to run PreAccept from scratch.
                RecoveryDecision::RunPreAccept
            }
        }
    }

    /// Collect all superseding transaction IDs reported across responses.
    pub fn superseding_txns(&self) -> Vec<TxnId> {
        let mut result: Vec<TxnId> = self
            .responses
            .iter()
            .flat_map(|r| r.superseding.iter().copied())
            .collect();
        result.sort();
        result.dedup();
        result
    }

    /// Collect all waiting transaction IDs reported across responses.
    pub fn waiting_txns(&self) -> Vec<TxnId> {
        let mut result: Vec<TxnId> = self
            .responses
            .iter()
            .flat_map(|r| r.waiting.iter().copied())
            .collect();
        result.sort();
        result.dedup();
        result
    }

    /// Number of responses collected so far.
    pub fn response_count(&self) -> usize {
        self.responses.len()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::{
        AcceptedBallot, BallotGenerator, BallotNumber, PromisedBallot, Timestamp, TxnId, TxnPhase,
        TxnState,
    };
    use std::collections::HashSet;

    /// Helper: build a TxnState with a specific accepted_ballot and values.
    fn state_with_accepted(
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: HashSet<TxnId>,
        accepted_ballot: BallotNumber,
        max_ballot_seen: BallotNumber,
    ) -> TxnState {
        let mut state = TxnState::new(txn_id, t0);
        state.t = t;
        state.deps = deps;
        // Set ballots — max_ballot_seen must be >= accepted_ballot.
        state.accepted_ballot = AcceptedBallot(accepted_ballot);
        state.max_ballot_seen = PromisedBallot(max_ballot_seen);
        if accepted_ballot != BallotNumber::default() {
            state.phase = TxnPhase::Accepted;
        }
        state
    }

    /// Helper: build a RecoverOKResponse from a TxnState.
    fn recover_ok(from: u64, state: TxnState) -> RecoverOKResponse {
        RecoverOKResponse {
            from,
            state,
            superseding: vec![],
            waiting: vec![],
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: recovery_selects_by_accepted_ballot
    // -----------------------------------------------------------------------

    /// Recovery MUST pick the value associated with the highest accepted_ballot,
    /// NOT the highest max_ballot_seen. This is the critical EPaxos/Accord
    /// correctness property.
    #[test]
    fn recovery_selects_by_accepted_ballot() {
        let ballot_gen = BallotGenerator::new();
        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        // Replica 1: accepted at ballot 5, but max_ballot_seen is 5.
        // Value: t=2000, deps={}.
        let t_high_accepted = Timestamp::synthetic(2000);
        let state1 = state_with_accepted(
            txn_id,
            t0,
            t_high_accepted,
            HashSet::new(),
            BallotNumber(5),
            BallotNumber(5),
        );
        assert!(coord.handle_recover_ok(recover_ok(1, state1)).is_none());

        // Replica 2: accepted at ballot 3 (lower), but max_ballot_seen is 10 (higher!).
        // Value: t=3000, deps={}.
        // A naive implementation would pick this because max_ballot_seen=10 > 5.
        let t_high_promised = Timestamp::synthetic(3000);
        let state2 = state_with_accepted(
            txn_id,
            t0,
            t_high_promised,
            HashSet::new(),
            BallotNumber(3),
            BallotNumber(10),
        );
        let decision = coord.handle_recover_ok(recover_ok(2, state2));

        // With 3 replicas, quorum is 2. We should have a decision now.
        assert!(decision.is_some(), "expected decision after quorum");

        match decision.unwrap() {
            RecoveryDecision::Commit { timestamp, .. } => {
                // MUST select the value from ballot 5 (t=2000), NOT ballot 3/promised 10 (t=3000).
                assert_eq!(
                    timestamp, t_high_accepted,
                    "recovery must select by accepted_ballot (5), not max_ballot_seen (10)"
                );
            }
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: recovery_majority_quorum
    // -----------------------------------------------------------------------

    /// Recovery requires a strict majority (quorum) of RecoverOK responses.
    #[test]
    fn recovery_majority_quorum() {
        let ballot_gen = BallotGenerator::new();
        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        // 5-node cluster: quorum is 3.
        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 5, &ballot_gen);

        let t = Timestamp::synthetic(2000);
        let make_state = || {
            state_with_accepted(
                txn_id,
                t0,
                t,
                HashSet::new(),
                BallotNumber(1),
                BallotNumber(1),
            )
        };

        // First two responses: no decision yet.
        assert!(coord
            .handle_recover_ok(recover_ok(1, make_state()))
            .is_none());
        assert!(coord
            .handle_recover_ok(recover_ok(2, make_state()))
            .is_none());

        // Third response: quorum reached, decision made.
        let decision = coord.handle_recover_ok(recover_ok(3, make_state()));
        assert!(
            decision.is_some(),
            "expected decision after 3 of 5 responses (quorum)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: recover_updates_promised_not_accepted
    // -----------------------------------------------------------------------

    /// When a replica handles a Recover message, it updates its promised ballot
    /// (max_ballot_seen) but does NOT update its accepted_ballot. This is
    /// essential: recovery is a "promise" phase, not an "accept" phase.
    #[test]
    fn recover_updates_promised_not_accepted() {
        use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};

        let mut cluster = TestCluster::new(3);

        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        // First, have replica 2 accept a value at ballot 2.
        let accept_msg = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::Accept {
                ballot: BallotNumber(2),
                txn_id,
                t0,
                t: Timestamp::synthetic(1500),
                deps: vec![],
            },
        };
        cluster.send(accept_msg);
        cluster.deliver_next(); // Delivers Accept to replica 2, enqueues AcceptOK.

        // Drain the AcceptOK response so the queue is clean.
        cluster.deliver_next(); // Delivers AcceptOK to replica 1 (coordinator).

        // Verify accepted_ballot is now 2.
        let state_before = cluster.replica(2).txn_states.get(&txn_id).unwrap();
        assert_eq!(
            state_before.accepted_ballot,
            AcceptedBallot(BallotNumber(2))
        );
        assert_eq!(
            state_before.max_ballot_seen,
            PromisedBallot(BallotNumber(2))
        );

        // Now send a Recover message with ballot 5.
        let recover_msg = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::Recover {
                ballot: BallotNumber(5),
                txn_id,
                t0,
            },
        };
        cluster.send(recover_msg);
        cluster.deliver_next(); // Delivers Recover to replica 2.

        // After recovery: max_ballot_seen should be updated to 5,
        // but accepted_ballot MUST remain at 2.
        let state_after = cluster.replica(2).txn_states.get(&txn_id).unwrap();
        assert_eq!(
            state_after.max_ballot_seen,
            PromisedBallot(BallotNumber(5)),
            "Recover should update promised ballot"
        );
        assert_eq!(
            state_after.accepted_ballot,
            AcceptedBallot(BallotNumber(2)),
            "Recover must NOT update accepted ballot"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: recover_nack_lower_ballot
    // -----------------------------------------------------------------------

    /// If a Recover message arrives with a ballot lower than the replica's
    /// current promised ballot, the replica NACKs with its current promised
    /// ballot.
    #[test]
    fn recover_nack_lower_ballot() {
        use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};

        let mut cluster = TestCluster::new(3);

        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        // First, have replica 2 promise ballot 10 via an Accept.
        let accept_msg = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::Accept {
                ballot: BallotNumber(10),
                txn_id,
                t0,
                t: Timestamp::synthetic(1500),
                deps: vec![],
            },
        };
        cluster.send(accept_msg);
        cluster.deliver_next(); // Delivers Accept to replica 2, enqueues AcceptOK.
        cluster.deliver_next(); // Delivers AcceptOK to replica 1 (drain it).

        // Now send a Recover with ballot 5 (lower than promised 10).
        let recover_msg = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::Recover {
                ballot: BallotNumber(5),
                txn_id,
                t0,
            },
        };
        cluster.send(recover_msg);
        let responses = cluster.deliver_next();

        // Should get a NACK.
        assert_eq!(responses.len(), 1);
        match &responses[0].payload {
            TestMessagePayload::Nack {
                txn_id: nack_txn_id,
                max_ballot_seen,
            } => {
                assert_eq!(*nack_txn_id, txn_id);
                assert_eq!(
                    *max_ballot_seen,
                    PromisedBallot(BallotNumber(10)),
                    "NACK should include the current promised ballot"
                );
            }
            other => panic!("expected Nack, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: recover_runs_preaccept_if_unseen
    // -----------------------------------------------------------------------

    /// If no replica has seen the transaction (all return default/zero state),
    /// recovery should decide to run PreAccept from scratch.
    #[test]
    fn recover_runs_preaccept_if_unseen() {
        let ballot_gen = BallotGenerator::new();
        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        // All replicas return default state (no accepted ballot).
        let unseen_state = TxnState::new(txn_id, t0);

        assert!(coord
            .handle_recover_ok(recover_ok(1, unseen_state.clone()))
            .is_none());

        let decision = coord.handle_recover_ok(recover_ok(2, unseen_state));
        assert!(decision.is_some());

        match decision.unwrap() {
            RecoveryDecision::RunPreAccept => {} // Correct!
            other => panic!(
                "expected RunPreAccept when no replica has seen the txn, got {:?}",
                other
            ),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: recover_reports_superseding_txns
    // -----------------------------------------------------------------------

    /// Recovery collects and deduplicates superseding transaction IDs from
    /// all responding replicas.
    #[test]
    fn recover_reports_superseding_txns() {
        let ballot_gen = BallotGenerator::new();
        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        let superseding_a = TxnId::new(2, Timestamp::synthetic(2000));
        let superseding_b = TxnId::new(3, Timestamp::synthetic(3000));

        // Replica 1 reports superseding_a.
        let mut resp1 = recover_ok(1, TxnState::new(txn_id, t0));
        resp1.superseding = vec![superseding_a];
        coord.handle_recover_ok(resp1);

        // Replica 2 reports superseding_a (duplicate) and superseding_b.
        let mut resp2 = recover_ok(2, TxnState::new(txn_id, t0));
        resp2.superseding = vec![superseding_a, superseding_b];
        coord.handle_recover_ok(resp2);

        let superseding = coord.superseding_txns();
        assert_eq!(superseding.len(), 2, "should deduplicate superseding txns");
        assert!(superseding.contains(&superseding_a));
        assert!(superseding.contains(&superseding_b));
    }

    // -----------------------------------------------------------------------
    // Test 7: recover_reports_waiting_txns
    // -----------------------------------------------------------------------

    /// Recovery collects and deduplicates waiting transaction IDs from
    /// all responding replicas.
    #[test]
    fn recover_reports_waiting_txns() {
        let ballot_gen = BallotGenerator::new();
        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        let mut coord = RecoveryCoordinator::start_recovery(txn_id, t0, 3, &ballot_gen);

        let waiting_a = TxnId::new(4, Timestamp::synthetic(4000));
        let waiting_b = TxnId::new(5, Timestamp::synthetic(5000));

        // Replica 1 reports waiting_a.
        let mut resp1 = recover_ok(1, TxnState::new(txn_id, t0));
        resp1.waiting = vec![waiting_a];
        coord.handle_recover_ok(resp1);

        // Replica 2 reports waiting_a (duplicate) and waiting_b.
        let mut resp2 = recover_ok(2, TxnState::new(txn_id, t0));
        resp2.waiting = vec![waiting_a, waiting_b];
        coord.handle_recover_ok(resp2);

        let waiting = coord.waiting_txns();
        assert_eq!(waiting.len(), 2, "should deduplicate waiting txns");
        assert!(waiting.contains(&waiting_a));
        assert!(waiting.contains(&waiting_b));
    }
}
