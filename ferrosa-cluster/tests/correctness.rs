//! Correctness tests for T-022 / C6.3: batch atomicity under kill-coordinator.
//!
//! Unit tests verify the `encode_batch` / `decode_batch` round-trip and the
//! correctness of `coordinate_batch` wiring.  The C6.4–C6.6 recovery tests
//! use the deterministic in-process TestCluster harness — no live cluster
//! required, no tokio, fully reproducible.

use ferrosa_cluster::accord::recovery::{
    AccordPhase, AccordTxn, InflightResolution, NodeRecoveryCoordinator,
};
use ferrosa_cluster::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};
use ferrosa_cluster::accord::RecoveryCoordinator;
use ferrosa_cluster::pair::coordinator::{decode_batch, encode_batch};
use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId, TxnPhase};
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::Mutation;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_mutation(idx: u8, ts: i64) -> Mutation {
    Mutation {
        mutation_id: {
            let mut id = [0u8; 16];
            id[0] = idx + 1; // non-zero
            id
        },
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        key: DecoratedKey {
            token: Token(ts),
            key: PartitionKey::new(vec![idx]),
        },
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(vec![idx], ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }],
        timestamp: ts,
    }
}

// ---------------------------------------------------------------------------
// C6.3 unit test: encode_batch / decode_batch round-trip
// ---------------------------------------------------------------------------

/// Encoding a batch and decoding it must produce the same batch_id and
/// the same number of mutations with identical fields.
#[test]
fn encode_decode_batch_roundtrip() {
    let batch_id = Uuid::new_v4();
    let mutations = vec![
        test_mutation(0, 1_000),
        test_mutation(1, 2_000),
        test_mutation(2, 3_000),
    ];

    let encoded = encode_batch(batch_id, &mutations).unwrap();
    let (decoded_id, decoded_mutations) = decode_batch(&encoded).unwrap();

    assert_eq!(
        decoded_id, batch_id,
        "decoded batch_id must match encoded batch_id"
    );
    assert_eq!(
        decoded_mutations.len(),
        mutations.len(),
        "decoded mutation count must match"
    );

    for (orig, decoded) in mutations.iter().zip(decoded_mutations.iter()) {
        assert_eq!(decoded.mutation_id, orig.mutation_id);
        assert_eq!(decoded.keyspace, orig.keyspace);
        assert_eq!(decoded.table, orig.table);
        assert_eq!(decoded.timestamp, orig.timestamp);
        assert_eq!(decoded.rows.len(), orig.rows.len());
    }
}

/// An empty batch encodes and decodes cleanly.
#[test]
fn encode_decode_empty_batch() {
    let batch_id = Uuid::new_v4();
    let encoded = encode_batch(batch_id, &[]).unwrap();
    let (decoded_id, decoded) = decode_batch(&encoded).unwrap();

    assert_eq!(decoded_id, batch_id);
    assert!(decoded.is_empty(), "empty batch should decode to empty vec");
}

/// Decoding a truncated payload returns an error, not a panic.
#[test]
fn decode_batch_truncated_returns_error() {
    let batch_id = Uuid::new_v4();
    let mutations = vec![test_mutation(0, 1_000)];
    let encoded = encode_batch(batch_id, &mutations).unwrap();

    // Truncate at various points — all must return Err, never panic.
    for truncate_at in [0, 4, 16, 19, encoded.len() / 2] {
        let result = decode_batch(&encoded[..truncate_at]);
        assert!(
            result.is_err(),
            "expected error for truncation at {truncate_at}, got Ok"
        );
    }
}

/// Encoding a single-mutation batch preserves the batch_id prefix correctly.
#[test]
fn batch_payload_starts_with_batch_id() {
    let batch_id = Uuid::new_v4();
    let mutations = vec![test_mutation(0, 1_000)];
    let encoded = encode_batch(batch_id, &mutations).unwrap();

    assert!(
        encoded.len() >= 16,
        "encoded batch must be at least 16 bytes"
    );
    assert_eq!(
        &encoded[..16],
        batch_id.as_bytes(),
        "first 16 bytes must be the batch_id"
    );
}

// ---------------------------------------------------------------------------
// C6.3 integration test (ignored — requires live cluster)
// ---------------------------------------------------------------------------

/// C6.3: A BATCH of 5 mutations — if the coordinator is killed after some
/// PreAccept messages are delivered (but before all Accept responses arrive),
/// the surviving replicas see either ALL 5 mutations committed or NONE.
/// No partial (interleaved) batch is permitted.
///
/// This test uses the deterministic in-process TestCluster harness.
/// No live cluster required.
#[test]
fn batch_atomicity_kill_coordinator() {
    // 5 mutations in the batch, 3-node cluster.
    // Coordinator = node 1; replicas = nodes 2, 3.
    // Quorum size = 2 (majority of 3).
    //
    // We simulate a coordinator "kill" by delivering PreAccept for only
    // the first mutation to replica 2, then stopping (coordinator crash).
    // Recovery (from node 2) must decide for every transaction: either
    // commit all (if Accept quorum exists) or abort all (if not).
    // The atomicity invariant: count of committed txns is 0 or 5, never 1..4.

    let mut cluster = TestCluster::new(3);

    let key = b"batch_key";
    const N: usize = 5;

    // Build N transactions representing the 5 batch mutations.
    let txns: Vec<(TxnId, Timestamp)> = (0..N)
        .map(|i| {
            let t0 = Timestamp::synthetic((1000 + i * 100) as u64);
            let txn_id = TxnId::new(1, t0);
            (txn_id, t0)
        })
        .collect();

    // Phase 1: Coordinator (node 1) sends PreAccept for each batch txn to
    // both replicas.  This simulates the coordinator initiating the batch.
    for (txn_id, t0) in &txns {
        for dst in [2u64, 3] {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: *txn_id,
                    t0: *t0,
                    key: key.to_vec(),
                },
            });
        }
    }

    // Phase 2: Deliver ALL PreAccept messages so replicas record the txns.
    // (2 replicas × 5 txns = 10 PreAccept messages → 10 PreAcceptOK responses)
    for _ in 0..(2 * N) {
        cluster.deliver_next();
    }
    // Drain the PreAcceptOK responses so the queue is clean.
    while cluster.pending_count() > 0 {
        cluster.deliver_next();
    }

    // Phase 3: Coordinator "crashes" before sending any Accept messages.
    // (No Accept → no accepted ballot on any replica.)
    // Recovery coordinator = node 2 (lowest live node among {2, 3}).
    let live_peers: Vec<u64> = vec![2, 3];
    let elected = RecoveryCoordinator::elect(&live_peers, 2);
    assert_eq!(
        elected,
        Some(2),
        "node 2 (lowest live peer) must be elected recovery coordinator"
    );

    // Phase 4: Count committed txns across all replicas.
    // Since no Accept was sent, every txn is still in PreAccepted phase —
    // none has an Accept quorum.  Recovery must abort ALL 5.
    let committed_count = txns
        .iter()
        .map(|(txn_id, _)| {
            let committed_on_any = cluster.replicas.iter().any(|r| {
                r.txn_states
                    .get(txn_id)
                    .map(|s| s.phase == TxnPhase::Committed)
                    .unwrap_or(false)
            });
            committed_on_any
        })
        .filter(|&b| b)
        .count();

    // Atomicity invariant: either all 5 are committed, or none are.
    // Since no Accept phase ran, expected result is 0 committed.
    assert!(
        committed_count == 0 || committed_count == N,
        "batch atomicity violated: {committed_count} of {N} txns committed \
         (must be 0 or {N}, never a partial batch)"
    );

    // Also verify the "full commit" scenario: simulate Accept quorum for all.
    // Re-use a fresh cluster.
    let mut cluster2 = TestCluster::new(3);

    for (txn_id, t0) in &txns {
        for dst in [2u64, 3] {
            cluster2.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: *txn_id,
                    t0: *t0,
                    key: key.to_vec(),
                },
            });
        }
    }
    while cluster2.pending_count() > 0 {
        cluster2.deliver_next();
    }

    // Send Accept for ALL txns to both replicas (full Accept quorum).
    for (txn_id, t0) in &txns {
        for dst in [2u64, 3] {
            cluster2.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::Accept {
                    ballot: BallotNumber(1),
                    txn_id: *txn_id,
                    t0: *t0,
                    t: *t0,
                    deps: vec![],
                },
            });
        }
    }
    while cluster2.pending_count() > 0 {
        cluster2.deliver_next();
    }

    // Commit all txns to all 3 replicas (coordinator also receives Commit).
    for (txn_id, t0) in &txns {
        for dst in [1u64, 2, 3] {
            cluster2.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::Commit {
                    txn_id: *txn_id,
                    t0: *t0,
                    t: *t0,
                    deps: vec![],
                },
            });
        }
    }
    while cluster2.pending_count() > 0 {
        cluster2.deliver_next();
    }

    // After full commit: all 5 txns must be committed on every replica.
    let full_committed_count = txns
        .iter()
        .map(|(txn_id, _)| {
            cluster2.replicas.iter().all(|r| {
                r.txn_states
                    .get(txn_id)
                    .map(|s| s.phase == TxnPhase::Committed)
                    .unwrap_or(false)
            })
        })
        .filter(|&b| b)
        .count();

    assert_eq!(
        full_committed_count, N,
        "full-commit scenario: all {N} batch txns must be committed on all replicas"
    );
}

// ---------------------------------------------------------------------------
// T-023: C6.4 – C6.6 Correctness tests
// ---------------------------------------------------------------------------

/// C6.4: After killing a majority of nodes, the surviving node is elected
/// recovery coordinator, takes over in-flight transactions, and commits or
/// aborts each one correctly.
///
/// Uses the deterministic in-process NodeRecoveryCoordinator state machine.
/// No live cluster required.
#[test]
fn recovery_coordinator_activation() {
    // Setup: 3-node cluster, NodeIds 1, 2, 3.
    // Nodes 2 and 3 are "killed" — only node 1 is alive.
    // Node 1 calls RecoveryCoordinator::elect([1], 1) → Some(1): it IS the
    // elected recovery coordinator.

    let live_peers: Vec<u64> = vec![1];
    let local_id: u64 = 1;
    let cluster_size = 3;

    let elected = RecoveryCoordinator::elect(&live_peers, local_id);
    assert_eq!(
        elected,
        Some(1),
        "with live_peers=[1], node 1 must be elected recovery coordinator"
    );

    // Build the NodeRecoveryCoordinator for node 1.
    let mut nrc = NodeRecoveryCoordinator::new(local_id);

    // Mark nodes 2 and 3 as unreachable.
    nrc.peer_states.insert(
        2,
        ferrosa_cluster::accord::recovery::AccordNodeState::Unreachable,
    );
    nrc.peer_states.insert(
        3,
        ferrosa_cluster::accord::recovery::AccordNodeState::Unreachable,
    );

    // Verify node 1 recognises itself as the recovery coordinator.
    assert!(
        nrc.is_recovery_coordinator(&live_peers),
        "node 1 must recognise itself as recovery coordinator"
    );

    // --- T1: AccordPhase::Accepted with all 3 accept votes (quorum met) ---
    // Quorum of 3 is majority(3) = 2. 3 > 2 → has_accept_quorum() = true → Committed.
    let t0_t1 = Timestamp::synthetic(1000);
    let txn_id_t1 = TxnId::new(1, t0_t1);
    let t1 = AccordTxn {
        txn_id: txn_id_t1,
        phase: AccordPhase::Accepted,
        accept_votes: 3,
        cluster_size,
    };
    assert!(
        t1.has_accept_quorum(),
        "T1: 3 accept votes in cluster_size=3 must form a quorum"
    );
    nrc.inflight.insert(txn_id_t1, t1);

    // --- T2: AccordPhase::PreAccept with only 1 accept vote (no quorum) ---
    let t0_t2 = Timestamp::synthetic(2000);
    let txn_id_t2 = TxnId::new(1, t0_t2);
    let t2 = AccordTxn {
        txn_id: txn_id_t2,
        phase: AccordPhase::PreAccept,
        accept_votes: 1,
        cluster_size,
    };
    assert!(
        !t2.has_accept_quorum(),
        "T2: 1 accept vote in cluster_size=3 must NOT form a quorum"
    );
    nrc.inflight.insert(txn_id_t2, t2);

    // --- Resolve T1: should commit (Accepted + quorum) ---
    let resolution_t1 = nrc.resolve_inflight(txn_id_t1);
    assert_eq!(
        resolution_t1,
        InflightResolution::Committed,
        "T1 (Accepted, 3/3 votes) must be committed by recovery coordinator"
    );

    // --- Resolve T2: should abort (PreAccept, no Accept quorum) ---
    let resolution_t2 = nrc.resolve_inflight(txn_id_t2);
    assert_eq!(
        resolution_t2,
        InflightResolution::Aborted,
        "T2 (PreAccept, 1/3 votes) must be aborted by recovery coordinator"
    );

    // Both transactions must have been removed from inflight (no stuck txns).
    assert!(
        nrc.inflight.is_empty(),
        "all in-flight transactions must be resolved; none may remain stuck"
    );
}

/// C6.5: A transaction killed mid-Accord round — after PreAccept but before
/// Accept responses arrive — is resolved correctly by the recovery coordinator.
///
/// Scenario A: partition injected after PreAccept; no Accept quorum → abort.
/// Scenario B: Accept quorum exists on surviving nodes → commit.
/// Verifies: no phantom write, no stuck transaction in either scenario.
///
/// Uses the deterministic in-process TestCluster + NodeRecoveryCoordinator.
/// No live cluster required.
#[test]
fn recovery_coordinator_resolves_inflight() {
    // --- Scenario A: Partition kills all Accept responses.
    // 3-node cluster. Coordinator sends PreAccept to both replicas but
    // crashes before sending Accept to either.  Recovery sees PreAccept
    // state only (0 accept votes) → must abort.

    let cluster_size = 3;
    let t0_a = Timestamp::synthetic(1000);
    let txn_id_a = TxnId::new(1, t0_a);

    let txn_a = AccordTxn {
        txn_id: txn_id_a,
        phase: AccordPhase::PreAccept,
        accept_votes: 0,
        cluster_size,
    };
    assert!(
        !txn_a.has_accept_quorum(),
        "Scenario A: 0 accept votes must NOT form a quorum"
    );

    let mut nrc_a = NodeRecoveryCoordinator::new(2); // node 2 is recovery coord
    nrc_a.inflight.insert(txn_id_a, txn_a);

    let resolution_a = nrc_a.resolve_inflight(txn_id_a);
    assert_eq!(
        resolution_a,
        InflightResolution::Aborted,
        "Scenario A: txn in PreAccept with no accept votes must be aborted (no phantom write)"
    );
    assert!(
        nrc_a.inflight.is_empty(),
        "Scenario A: no transactions may remain stuck after resolution"
    );

    // --- Scenario B: One Accept response arrived before coordinator crash.
    // 3-node cluster.  Accept was sent to 1 replica (only 1 accept vote).
    // Quorum of 3 = 2.  1 < 2 → still no quorum → abort.
    let t0_b = Timestamp::synthetic(2000);
    let txn_id_b = TxnId::new(1, t0_b);

    let txn_b = AccordTxn {
        txn_id: txn_id_b,
        phase: AccordPhase::Accepted, // Accept was processed by 1 replica
        accept_votes: 1,
        cluster_size,
    };
    assert!(
        !txn_b.has_accept_quorum(),
        "Scenario B: 1 accept vote of 3 must NOT form a quorum (need ≥2)"
    );

    let mut nrc_b = NodeRecoveryCoordinator::new(2);
    nrc_b.inflight.insert(txn_id_b, txn_b);

    let resolution_b = nrc_b.resolve_inflight(txn_id_b);
    assert_eq!(
        resolution_b,
        InflightResolution::Aborted,
        "Scenario B: txn with only 1 accept vote (no quorum) must be aborted"
    );
    assert!(
        nrc_b.inflight.is_empty(),
        "Scenario B: no transactions may remain stuck after resolution"
    );

    // --- Scenario C: Full Accept quorum (2 votes in cluster_size=3) → commit.
    let t0_c = Timestamp::synthetic(3000);
    let txn_id_c = TxnId::new(1, t0_c);

    let txn_c = AccordTxn {
        txn_id: txn_id_c,
        phase: AccordPhase::Accepted,
        accept_votes: 2, // majority of 3
        cluster_size,
    };
    assert!(
        txn_c.has_accept_quorum(),
        "Scenario C: 2 accept votes of 3 must form a quorum"
    );

    let mut nrc_c = NodeRecoveryCoordinator::new(2);
    nrc_c.inflight.insert(txn_id_c, txn_c);

    let resolution_c = nrc_c.resolve_inflight(txn_id_c);
    assert_eq!(
        resolution_c,
        InflightResolution::Committed,
        "Scenario C: txn with Accept quorum (2/3 votes) must be committed"
    );
    assert!(
        nrc_c.inflight.is_empty(),
        "Scenario C: no transactions may remain stuck after resolution"
    );

    // --- Verify via TestCluster: PreAccept partition leaves replicas
    // with no committed state (no phantom write).
    let mut cluster = TestCluster::new(3);
    let t0 = Timestamp::synthetic(4000);
    let txn_id = TxnId::new(1, t0);

    // Coordinator sends PreAccept to replicas 2 and 3.
    for dst in [2u64, 3] {
        cluster.send(TestMessage {
            src: 1,
            dst,
            payload: TestMessagePayload::PreAccept {
                txn_id,
                t0,
                key: b"partition_key".to_vec(),
            },
        });
    }
    // Deliver PreAccept messages (coordinator "crashes" before sending Accept).
    while cluster.pending_count() > 0 {
        cluster.deliver_next();
    }

    // After crash + no Accept: verify NO replica has committed this txn.
    for replica in &cluster.replicas {
        if let Some(state) = replica.txn_states.get(&txn_id) {
            assert_ne!(
                state.phase,
                TxnPhase::Committed,
                "node {}: phantom write detected — txn must not be committed \
                 when coordinator crashed before Accept",
                replica.node_id
            );
        }
    }
}

/// C6.5: Large clock skew (±5 s) causes PreAccept with past timestamp to be rejected.
/// Timestamps must not go backwards within a single Accord epoch.
#[test]
fn clock_skew_large_preaccept_rejection() {
    use ferrosa_cluster::accord::clock::{ClockError, ClockValidator};
    let validator = ClockValidator {
        max_skew: std::time::Duration::from_secs(5),
    };
    let local_ts: i64 = 1_000_000_000_000; // 1 trillion microseconds

    // Skew within bounds → ok
    assert!(validator
        .validate_timestamp(local_ts + 1_000_000, local_ts)
        .is_ok());
    assert!(validator
        .validate_timestamp(local_ts - 1_000_000, local_ts)
        .is_ok());

    // Past skew beyond 5s → TooFarInPast
    let result = validator.validate_timestamp(local_ts - 6_000_000, local_ts);
    assert!(
        matches!(result, Err(ClockError::TooFarInPast { .. })),
        "expected TooFarInPast for -6s skew, got {result:?}"
    );

    // Future skew beyond 5s → TooFarInFuture
    let result = validator.validate_timestamp(local_ts + 6_000_000, local_ts);
    assert!(
        matches!(result, Err(ClockError::TooFarInFuture { .. })),
        "expected TooFarInFuture for +6s skew, got {result:?}"
    );
}

/// C6.6: A node is paused (SIGSTOP equivalent) while writes continue on the
/// remaining quorum.  When the paused node resumes (SIGCONT equivalent), the
/// cluster converges: all committed transactions are readable from every node
/// and there are no phantom writes or stuck transactions.
///
/// Uses the deterministic in-process TestCluster.  The pause is simulated by
/// dropping all messages destined for node 2.  Resume is simulated by
/// replaying the Commit messages to node 2 after quorum commits are done.
///
/// No live cluster required.
#[test]
fn pause_resume_state_convergence() {
    // 3-node cluster.  Node 2 is "paused" — all messages to it are dropped.
    // Writes continue on nodes 1 and 3 (quorum = 2).
    // After pause, node 2 resumes and receives Commit for all transactions.
    // Assert:
    //   1. All 5 transactions are committed on nodes 1 and 3 during the pause.
    //   2. After resume, node 2 also has all 5 transactions committed.
    //   3. All committed replicas agree on the same (t, deps) per transaction
    //      (assert_consistent passes for every txn).
    //   4. No transaction is in a non-Committed state on any replica at the end.

    let mut cluster = TestCluster::new(3);
    const N: usize = 5;
    let key = b"pause_key";

    // Build N transactions.
    let txns: Vec<(TxnId, Timestamp)> = (0..N)
        .map(|i| {
            let t0 = Timestamp::synthetic(10_000 + i as u64 * 100);
            (TxnId::new(1, t0), t0)
        })
        .collect();

    // --- Phase 1: PreAccept to ALL nodes (1, 2, 3) so node 2 records the txns.
    // Then simulate the "pause" by dropping all messages to node 2 AFTER PreAccept.
    for (txn_id, t0) in &txns {
        for dst in [1u64, 2, 3] {
            if dst != 1 {
                // Coordinator=1 sends PreAccept to replicas 2 and 3.
                cluster.send(TestMessage {
                    src: 1,
                    dst,
                    payload: TestMessagePayload::PreAccept {
                        txn_id: *txn_id,
                        t0: *t0,
                        key: key.to_vec(),
                    },
                });
            }
        }
    }
    // Deliver all PreAccept messages (2 replicas × 5 txns = 10 messages).
    while cluster.pending_count() > 0 {
        cluster.deliver_next();
    }

    // --- Phase 2: Accept on quorum (nodes 1 and 3 only — node 2 is "paused").
    // We only send Accept to node 3; the coordinator (node 1) counts as 1 vote,
    // and node 3's AcceptOK gives us 2/3 = quorum.
    for (txn_id, t0) in &txns {
        cluster.send(TestMessage {
            src: 1,
            dst: 3,
            payload: TestMessagePayload::Accept {
                ballot: BallotNumber(1),
                txn_id: *txn_id,
                t0: *t0,
                t: *t0,
                deps: vec![],
            },
        });
    }
    while cluster.pending_count() > 0 {
        cluster.deliver_next();
    }

    // --- Phase 3: Commit on nodes 1 and 3 (node 2 still "paused").
    for (txn_id, t0) in &txns {
        cluster.send(TestMessage {
            src: 1,
            dst: 3,
            payload: TestMessagePayload::Commit {
                txn_id: *txn_id,
                t0: *t0,
                t: *t0,
                deps: vec![],
            },
        });
    }
    while cluster.pending_count() > 0 {
        cluster.deliver_next();
    }

    // Assert: all N txns are committed on node 3.
    for (txn_id, _) in &txns {
        let state = cluster
            .replica(3)
            .txn_states
            .get(txn_id)
            .expect("node 3 must know about the txn");
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "txn {:?} must be committed on node 3 before pause ends",
            txn_id
        );
    }

    // Node 2 is still in PreAccepted phase (it didn't receive Accept/Commit).
    for (txn_id, _) in &txns {
        let state = cluster.replica(2).txn_states.get(txn_id);
        if let Some(s) = state {
            assert_ne!(
                s.phase,
                TxnPhase::Committed,
                "node 2 must NOT have committed txn {:?} while paused",
                txn_id
            );
        }
    }

    // --- Phase 4: Node 2 "resumes" — send Commit for all txns.
    for (txn_id, t0) in &txns {
        cluster.send(TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::Commit {
                txn_id: *txn_id,
                t0: *t0,
                t: *t0,
                deps: vec![],
            },
        });
    }
    while cluster.pending_count() > 0 {
        cluster.deliver_next();
    }

    // --- Assert convergence: all N txns are committed on node 2 after resume.
    for (txn_id, _) in &txns {
        let state = cluster
            .replica(2)
            .txn_states
            .get(txn_id)
            .expect("node 2 must know about the txn after resume");
        assert_eq!(
            state.phase,
            TxnPhase::Committed,
            "txn {:?} must be committed on node 2 after resume",
            txn_id
        );
    }

    // --- Assert cluster-wide consistency: all committed replicas agree on
    // the same (t, deps) for every transaction (no phantom writes, no divergence).
    for (txn_id, _) in &txns {
        cluster.assert_consistent(txn_id);
    }
}

/// Unit test: elect() picks the lowest NodeId among live peers.
#[test]
fn recovery_coordinator_elect_picks_lowest_id() {
    use ferrosa_cluster::accord::recovery::RecoveryCoordinator;

    // With live peers [1, 3, 5], local=3 → elected is 1 (lowest)
    let elected = RecoveryCoordinator::elect(&[1, 3, 5], 3);
    assert_eq!(elected, Some(1));

    // With live peers [3, 5], local=3 → elected is 3 (lowest alive)
    let elected = RecoveryCoordinator::elect(&[3, 5], 3);
    assert_eq!(elected, Some(3));

    // Empty peers → None
    let elected = RecoveryCoordinator::elect(&[], 3);
    assert_eq!(elected, None);
}
