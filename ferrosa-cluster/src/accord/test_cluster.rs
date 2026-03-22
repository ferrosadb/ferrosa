//! Deterministic message harness for Accord protocol testing.
//!
//! [`TestCluster`] simulates multiple Accord replicas with explicit message
//! scheduling. No real network, no tokio, no timers — everything is synchronous
//! and deterministic. This is the foundation for the 24-step EPaxos correctness
//! test and all protocol scenario tests.
//!
//! This module lives in the main source (not behind `#[cfg(test)]`) because it
//! is used by integration tests in `tests/` as well.

use ferrosa_common::accord::{
    AcceptedBallot, BallotNumber, PromisedBallot, Timestamp, TxnId, TxnPhase, TxnState,
};
use std::collections::{HashMap, VecDeque};

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// A message between replicas in the test cluster.
#[derive(Debug, Clone)]
pub struct TestMessage {
    /// Source node ID.
    pub src: u64,
    /// Destination node ID.
    pub dst: u64,
    /// Protocol payload.
    pub payload: TestMessagePayload,
}

/// Protocol message payloads for testing.
#[derive(Debug, Clone)]
pub enum TestMessagePayload {
    PreAccept {
        txn_id: TxnId,
        t0: Timestamp,
        /// Simplified: single key per transaction.
        key: Vec<u8>,
    },
    PreAcceptOK {
        txn_id: TxnId,
        t: Timestamp,
        deps: Vec<TxnId>,
    },
    Accept {
        ballot: BallotNumber,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: Vec<TxnId>,
    },
    AcceptOK {
        txn_id: TxnId,
        ballot: BallotNumber,
        deps: Vec<TxnId>,
    },
    Commit {
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: Vec<TxnId>,
    },
    Recover {
        ballot: BallotNumber,
        txn_id: TxnId,
        t0: Timestamp,
    },
    RecoverOK {
        txn_id: TxnId,
        state: TxnState,
        superseding: Vec<TxnId>,
        wait: Vec<TxnId>,
    },
    Nack {
        txn_id: TxnId,
        max_ballot_seen: PromisedBallot,
    },
}

// ---------------------------------------------------------------------------
// TestReplica
// ---------------------------------------------------------------------------

/// A simulated Accord replica for testing.
#[derive(Debug)]
pub struct TestReplica {
    /// This replica's node ID.
    pub node_id: u64,
    /// Per-transaction state tracked by this replica.
    pub txn_states: HashMap<TxnId, TxnState>,
    /// Simple conflict tracking: key -> list of (t0, txn_id).
    pub conflicts: HashMap<Vec<u8>, Vec<(Timestamp, TxnId)>>,
}

impl TestReplica {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            txn_states: HashMap::new(),
            conflicts: HashMap::new(),
        }
    }

    /// Handle an incoming message. Returns response messages to enqueue.
    pub fn handle(&mut self, msg: &TestMessage) -> Vec<TestMessage> {
        assert_eq!(
            msg.dst, self.node_id,
            "message delivered to wrong replica: dst={} but node_id={}",
            msg.dst, self.node_id
        );

        match &msg.payload {
            TestMessagePayload::PreAccept { txn_id, t0, key } => {
                self.handle_preaccept(msg.src, *txn_id, *t0, key)
            }
            TestMessagePayload::Accept {
                ballot,
                txn_id,
                t0,
                t,
                deps,
            } => self.handle_accept(msg.src, *ballot, *txn_id, *t0, *t, deps),
            TestMessagePayload::Recover { ballot, txn_id, t0 } => {
                self.handle_recover(msg.src, *ballot, *txn_id, *t0)
            }
            TestMessagePayload::Commit {
                txn_id,
                t0,
                t,
                deps,
            } => {
                self.handle_commit(*txn_id, *t0, *t, deps);
                vec![]
            }
            // PreAcceptOK, AcceptOK, RecoverOK, Nack are coordinator-side
            // messages that don't generate responses from the replica.
            _ => vec![],
        }
    }

    /// Handle a PreAccept message.
    ///
    /// Check conflicts for the key. If no conflicting transaction has a higher t0,
    /// agree with the proposed timestamp (t == t0). Otherwise, propose a higher t.
    /// Return PreAcceptOK with dependency list.
    fn handle_preaccept(
        &mut self,
        from: u64,
        txn_id: TxnId,
        t0: Timestamp,
        key: &[u8],
    ) -> Vec<TestMessage> {
        // Collect dependencies: all transactions touching this key.
        let conflicts = self.conflicts.get(key).cloned().unwrap_or_default();

        let deps: Vec<TxnId> = conflicts
            .iter()
            .filter(|(_, id)| *id != txn_id)
            .map(|(_, id)| *id)
            .collect();

        // Determine execution timestamp: max of t0 and all conflicting t0 values.
        let max_conflict_t0 = conflicts
            .iter()
            .filter(|(_, id)| *id != txn_id)
            .map(|(ts, _)| *ts)
            .max();

        let t = match max_conflict_t0 {
            Some(ct) if ct > t0 => ct.bump_past(&ct, self.node_id),
            _ => t0,
        };

        // Record state for this transaction.
        let mut state = TxnState::new(txn_id, t0);
        state.t = t;
        state.deps = deps.iter().copied().collect();
        self.txn_states.insert(txn_id, state);

        // Track this transaction in the conflict index.
        self.conflicts
            .entry(key.to_vec())
            .or_default()
            .push((t0, txn_id));

        vec![TestMessage {
            src: self.node_id,
            dst: from,
            payload: TestMessagePayload::PreAcceptOK { txn_id, t, deps },
        }]
    }

    /// Handle an Accept message.
    ///
    /// If the ballot is >= the replica's promised ballot, accept the value and
    /// update both ballot fields. Otherwise, NACK with the current promised ballot.
    fn handle_accept(
        &mut self,
        from: u64,
        ballot: BallotNumber,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: &[TxnId],
    ) -> Vec<TestMessage> {
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| TxnState::new(txn_id, t0));

        // Check ballot against promised ballot.
        if ballot < state.max_ballot_seen.0 {
            return vec![TestMessage {
                src: self.node_id,
                dst: from,
                payload: TestMessagePayload::Nack {
                    txn_id,
                    max_ballot_seen: state.max_ballot_seen,
                },
            }];
        }

        // Accept: update both ballot fields, timestamp, deps, and phase.
        state.accepted_ballot = AcceptedBallot(ballot);
        state.max_ballot_seen = PromisedBallot(ballot);
        state.t = t;
        state.deps = deps.iter().copied().collect();
        state.phase = TxnPhase::Accepted;

        vec![TestMessage {
            src: self.node_id,
            dst: from,
            payload: TestMessagePayload::AcceptOK {
                txn_id,
                ballot,
                deps: deps.to_vec(),
            },
        }]
    }

    /// Handle a Recover message.
    ///
    /// Update promised_ballot (NOT accepted_ballot) if the recovery ballot is
    /// higher. Return RecoverOK with the current transaction state.
    fn handle_recover(
        &mut self,
        from: u64,
        ballot: BallotNumber,
        txn_id: TxnId,
        t0: Timestamp,
    ) -> Vec<TestMessage> {
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| TxnState::new(txn_id, t0));

        // Update promised ballot if recovery ballot is higher.
        if ballot > state.max_ballot_seen.0 {
            state.max_ballot_seen = PromisedBallot(ballot);
        }

        // The state we return is the current state (not modified by recovery
        // ballot — accepted_ballot stays the same).
        vec![TestMessage {
            src: self.node_id,
            dst: from,
            payload: TestMessagePayload::RecoverOK {
                txn_id,
                state: state.clone(),
                superseding: vec![],
                wait: vec![],
            },
        }]
    }

    /// Handle a Commit message (no response generated).
    fn handle_commit(&mut self, txn_id: TxnId, t0: Timestamp, t: Timestamp, deps: &[TxnId]) {
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| TxnState::new(txn_id, t0));
        state.phase = TxnPhase::Committed;
        state.t = t;
        state.deps = deps.iter().copied().collect();
    }
}

// ---------------------------------------------------------------------------
// TestCluster
// ---------------------------------------------------------------------------

/// Deterministic message scheduler for Accord protocol testing.
///
/// Messages are enqueued explicitly and delivered only when the test calls
/// `deliver_next()`, `deliver_at()`, or `drain()`. This gives tests full
/// control over message ordering, enabling deterministic reproduction of
/// race conditions, partitions, and reordering scenarios.
pub struct TestCluster {
    /// The simulated replicas (indexed by position, node IDs are 1-based).
    pub replicas: Vec<TestReplica>,
    /// Pending messages awaiting delivery.
    pending: VecDeque<TestMessage>,
}

impl TestCluster {
    /// Create a cluster with `n` replicas (node IDs 1..=n).
    pub fn new(n: usize) -> Self {
        assert!(n > 0, "cluster must have at least one replica");
        let replicas = (1..=n).map(|id| TestReplica::new(id as u64)).collect();
        Self {
            replicas,
            pending: VecDeque::new(),
        }
    }

    /// Enqueue a message. Not delivered until deliver methods are called.
    pub fn send(&mut self, msg: TestMessage) {
        self.pending.push_back(msg);
    }

    /// Deliver the next message in FIFO order. Returns response messages
    /// (which are also automatically enqueued).
    ///
    /// # Panics
    ///
    /// Panics if no messages are pending.
    pub fn deliver_next(&mut self) -> Vec<TestMessage> {
        let msg = self
            .pending
            .pop_front()
            .expect("no pending messages to deliver");
        self.deliver_message(&msg)
    }

    /// Deliver the message at a specific index (for out-of-order testing).
    /// Index 0 is the front of the queue.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn deliver_at(&mut self, index: usize) -> Vec<TestMessage> {
        assert!(
            index < self.pending.len(),
            "index {} out of bounds, {} pending",
            index,
            self.pending.len()
        );
        let msg = self.pending.remove(index).unwrap();
        self.deliver_message(&msg)
    }

    /// Drop a message at a specific index (simulate network partition).
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn drop_at(&mut self, index: usize) {
        assert!(
            index < self.pending.len(),
            "index {} out of bounds, {} pending",
            index,
            self.pending.len()
        );
        self.pending.remove(index);
    }

    /// Deliver all pending messages until quiescent (no more pending).
    ///
    /// Messages generated by handlers are also delivered. Uses a bounded
    /// iteration limit to prevent infinite loops.
    pub fn drain(&mut self) {
        const MAX_ITERATIONS: usize = 10_000;
        let mut iterations = 0;
        while !self.pending.is_empty() {
            assert!(
                iterations < MAX_ITERATIONS,
                "drain() exceeded {} iterations — possible infinite loop",
                MAX_ITERATIONS
            );
            self.deliver_next();
            iterations += 1;
        }
    }

    /// Number of pending messages.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get a replica by node ID.
    ///
    /// # Panics
    ///
    /// Panics if no replica with the given node ID exists.
    pub fn replica(&self, node_id: u64) -> &TestReplica {
        self.replicas
            .iter()
            .find(|r| r.node_id == node_id)
            .unwrap_or_else(|| panic!("no replica with node_id={}", node_id))
    }

    /// Get a mutable replica by node ID.
    ///
    /// # Panics
    ///
    /// Panics if no replica with the given node ID exists.
    pub fn replica_mut(&mut self, node_id: u64) -> &mut TestReplica {
        self.replicas
            .iter_mut()
            .find(|r| r.node_id == node_id)
            .unwrap_or_else(|| panic!("no replica with node_id={}", node_id))
    }

    /// Assert all replicas that know about a transaction agree on committed state.
    ///
    /// Only checks replicas that have the transaction in their state map and
    /// are in the `Committed` phase. Verifies they agree on `t` (execution
    /// timestamp) and `deps` (dependency set).
    ///
    /// # Panics
    ///
    /// Panics if any two committed replicas disagree on `t` or `deps`.
    pub fn assert_consistent(&self, txn_id: &TxnId) {
        let committed_states: Vec<&TxnState> = self
            .replicas
            .iter()
            .filter_map(|r| r.txn_states.get(txn_id))
            .filter(|s| s.phase == TxnPhase::Committed)
            .collect();

        if committed_states.len() < 2 {
            return; // Nothing to compare
        }

        let reference = committed_states[0];
        for (i, state) in committed_states.iter().enumerate().skip(1) {
            assert_eq!(
                reference.t, state.t,
                "replicas disagree on execution timestamp for txn {:?}: \
                 replica 0 has {:?}, replica {} has {:?}",
                txn_id, reference.t, i, state.t
            );

            // HashSet equality is order-independent — compare directly.
            assert_eq!(
                reference.deps,
                state.deps,
                "replicas disagree on deps for txn {:?}: \
                 replica 0 has {} deps, replica {} has {} deps",
                txn_id,
                reference.deps.len(),
                i,
                state.deps.len()
            );
        }
    }

    /// Internal: deliver a message to its destination replica and enqueue responses.
    fn deliver_message(&mut self, msg: &TestMessage) -> Vec<TestMessage> {
        let replica = self
            .replicas
            .iter_mut()
            .find(|r| r.node_id == msg.dst)
            .unwrap_or_else(|| panic!("no replica with node_id={}", msg.dst));

        let responses = replica.handle(msg);

        // Enqueue response messages for later delivery.
        for response in &responses {
            self.pending.push_back(response.clone());
        }

        responses
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;
    use std::collections::HashSet;

    /// Helper: create a simple PreAccept test message.
    fn preaccept_msg(src: u64, dst: u64, t0_micros: u64) -> TestMessage {
        let t0 = Timestamp::synthetic(t0_micros);
        let txn_id = TxnId::new(src, t0);
        TestMessage {
            src,
            dst,
            payload: TestMessagePayload::PreAccept {
                txn_id,
                t0,
                key: b"key1".to_vec(),
            },
        }
    }

    /// Helper: create a simple Commit test message (generates no response).
    fn commit_msg(src: u64, dst: u64, t0_micros: u64) -> TestMessage {
        let t0 = Timestamp::synthetic(t0_micros);
        let txn_id = TxnId::new(src, t0);
        TestMessage {
            src,
            dst,
            payload: TestMessagePayload::Commit {
                txn_id,
                t0,
                t: t0,
                deps: vec![],
            },
        }
    }

    #[test]
    fn test_cluster_deterministic_delivery() {
        let mut cluster = TestCluster::new(3);

        // Enqueue 3 messages to different replicas.
        let msg1 = preaccept_msg(1, 2, 100);
        let msg2 = preaccept_msg(1, 3, 200);
        let msg3 = preaccept_msg(2, 1, 300);

        // Capture txn_ids to verify FIFO order by checking which replica
        // processed each message.
        let dst1 = msg1.dst;
        let dst2 = msg2.dst;
        let dst3 = msg3.dst;

        cluster.send(msg1);
        cluster.send(msg2);
        cluster.send(msg3);

        assert_eq!(cluster.pending_count(), 3);

        // deliver_next() should deliver in FIFO order.
        let r1 = cluster.deliver_next();
        assert_eq!(r1.len(), 1); // PreAcceptOK response
        assert_eq!(r1[0].src, dst1); // Response came from replica 2

        let r2 = cluster.deliver_next();
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].src, dst2); // Response came from replica 3

        let r3 = cluster.deliver_next();
        assert_eq!(r3.len(), 1);
        assert_eq!(r3[0].src, dst3); // Response came from replica 1

        // The 3 responses are now pending (one from each deliver_next).
        assert_eq!(cluster.pending_count(), 3);
    }

    #[test]
    fn test_cluster_out_of_order_delivery() {
        let mut cluster = TestCluster::new(3);

        let msg1 = preaccept_msg(1, 2, 100);
        let msg2 = preaccept_msg(1, 3, 200);
        let msg3 = preaccept_msg(2, 1, 300);

        let dst3 = msg3.dst; // node 1

        cluster.send(msg1);
        cluster.send(msg2);
        cluster.send(msg3);

        assert_eq!(cluster.pending_count(), 3);

        // Deliver message at index 2 (msg3) first — out of order.
        let r = cluster.deliver_at(2);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].src, dst3); // Response from replica 1

        // Messages 1 and 2 are still pending, plus the response from msg3.
        assert_eq!(cluster.pending_count(), 3); // msg1, msg2, and response
    }

    #[test]
    fn test_cluster_drop_message() {
        let mut cluster = TestCluster::new(3);

        let msg1 = preaccept_msg(1, 2, 100);
        let msg2 = preaccept_msg(1, 3, 200);
        let msg3 = preaccept_msg(2, 1, 300);

        let dst1 = msg1.dst; // node 2
        let dst3 = msg3.dst; // node 1

        cluster.send(msg1);
        cluster.send(msg2);
        cluster.send(msg3);

        assert_eq!(cluster.pending_count(), 3);

        // Drop message at index 1 (msg2) — simulates partition.
        cluster.drop_at(1);
        assert_eq!(cluster.pending_count(), 2);

        // Deliver remaining: should be msg1 then msg3.
        let r1 = cluster.deliver_next();
        assert_eq!(r1[0].src, dst1); // From replica 2 (msg1's dst)

        // After delivering msg1, its response is enqueued, so we have
        // msg3 + response = 2 pending.
        let r3 = cluster.deliver_next();
        assert_eq!(r3[0].src, dst3); // From replica 1 (msg3's dst)
    }

    #[test]
    fn test_cluster_drain() {
        let mut cluster = TestCluster::new(5);

        // Enqueue 10 PreAccept messages to different replicas.
        for i in 0..10 {
            let src = 1;
            let dst = (i % 5) as u64 + 1;
            // If src == dst, pick a different dst.
            let dst = if dst == src { (dst % 5) + 1 } else { dst };
            let msg = preaccept_msg(src, dst, (i + 1) as u64 * 100);
            cluster.send(msg);
        }

        assert_eq!(cluster.pending_count(), 10);

        // drain() should deliver all messages (and their responses) until quiescent.
        cluster.drain();

        assert_eq!(cluster.pending_count(), 0);
    }

    #[test]
    fn test_cluster_assert_consistent() {
        let mut cluster = TestCluster::new(3);

        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        // Commit the same transaction on replicas 1 and 2 with same state.
        for node_id in [1u64, 2] {
            let replica = cluster.replica_mut(node_id);
            let mut state = TxnState::new(txn_id, t0);
            state.phase = TxnPhase::Committed;
            state.t = t0;
            state.deps = HashSet::new();
            replica.txn_states.insert(txn_id, state);
        }

        // Should pass — both replicas agree.
        cluster.assert_consistent(&txn_id);

        // Now modify replica 2's execution timestamp to create inconsistency.
        let replica2 = cluster.replica_mut(2);
        let state2 = replica2.txn_states.get_mut(&txn_id).unwrap();
        state2.t = Timestamp::synthetic(9999);

        // Should panic — replicas disagree on `t`.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cluster.assert_consistent(&txn_id);
        }));
        assert!(
            result.is_err(),
            "assert_consistent should panic when replicas disagree"
        );
    }

    #[test]
    fn test_cluster_no_tokio() {
        // This test proves the TestCluster works without a tokio runtime.
        // No #[tokio::test], no async, no Runtime::new(). Pure synchronous Rust.

        let mut cluster = TestCluster::new(3);

        let t0 = Timestamp::synthetic(500);
        let txn_id = TxnId::new(1, t0);

        // Send a PreAccept.
        cluster.send(TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::PreAccept {
                txn_id,
                t0,
                key: b"no_tokio_key".to_vec(),
            },
        });

        // Deliver it synchronously.
        let responses = cluster.deliver_next();
        assert_eq!(responses.len(), 1);

        // Verify the response is a PreAcceptOK.
        match &responses[0].payload {
            TestMessagePayload::PreAcceptOK {
                txn_id: resp_txn_id,
                t,
                deps,
            } => {
                assert_eq!(*resp_txn_id, txn_id);
                assert_eq!(*t, t0); // No conflicts, so t == t0.
                assert!(deps.is_empty());
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }

        // Verify replica state was updated.
        let replica = cluster.replica(2);
        assert!(replica.txn_states.contains_key(&txn_id));
        assert_eq!(replica.txn_states[&txn_id].phase, TxnPhase::PreAccepted);

        // All synchronous — no tokio runtime needed.
    }
}
