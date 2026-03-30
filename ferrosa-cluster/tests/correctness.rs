//! Correctness tests for T-022 / C6.3: batch atomicity under kill-coordinator.
//!
//! Unit tests verify the `encode_batch` / `decode_batch` round-trip and the
//! correctness of `coordinate_batch` wiring.  Integration tests panic with setup
//! instructions when cluster infrastructure is not available.

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::Mutation;
use uuid::Uuid;

use ferrosa_cluster::pair::coordinator::{decode_batch, encode_batch};

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

/// C6.3: A BATCH of 3 rows — if the coordinator is killed after the first row
/// is written, the surviving nodes see either all 3 rows or none.
///
/// Requires a live Accord cluster. Set FERROSA_TEST_CLUSTER_NODES to run.
#[tokio::test]
async fn batch_atomicity_kill_coordinator() {
    if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
    {
        panic!(
            "batch_atomicity_kill_coordinator requires a live Accord cluster — \
             set FERROSA_TEST_CLUSTER_NODES or run scripts/lima-fc-cluster-up.sh \
             and set FERROSA_TEST_FIRECRACKER=1"
        );
    }
    todo!("requires live cluster with fault injection and FERROSA_DATA_DIR set")
}

// ---------------------------------------------------------------------------
// T-023: C6.4 – C6.6 Correctness tests
// ---------------------------------------------------------------------------

/// C6.4: After killing majority of nodes, recovery coordinator gets elected,
/// takes over inflight transactions, and commits or aborts each one.
#[tokio::test]
async fn recovery_coordinator_activation() {
    if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
    {
        panic!(
            "recovery_coordinator_activation requires a live 3-node cluster — \
             set FERROSA_TEST_CLUSTER_NODES or run scripts/lima-fc-cluster-up.sh \
             and set FERROSA_TEST_FIRECRACKER=1"
        );
    }
    // Setup: mock 3-node cluster with NodeId 1, 2, 3
    // Kill nodes 2 and 3 (majority dead)
    // Node 1 calls RecoveryCoordinator::elect([1], 1) → Some(1) (it's the coordinator)
    // In-flight txn T1 has AccordPhase::Accepted with all 3 accept votes → commit
    // In-flight txn T2 has AccordPhase::PreAccept with only 1 vote → abort
    // Assert: T1 committed, T2 aborted, no transaction stuck in PreAccept
    todo!("requires live 3-node cluster")
}

/// C6.5: A txn that was killed mid-Accord round (after PreAccept but before Accept)
/// is recovered by the coordinator: either committed if quorum Accept exists, else aborted.
#[tokio::test]
async fn recovery_coordinator_resolves_inflight() {
    if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
    {
        panic!(
            "recovery_coordinator_resolves_inflight requires a live 3-node cluster — \
             set FERROSA_TEST_CLUSTER_NODES or run scripts/lima-fc-cluster-up.sh \
             and set FERROSA_TEST_FIRECRACKER=1"
        );
    }
    // 3 nodes. Inject a partition after PreAccept is sent but before Accept responses.
    // Recovery coordinator resolves: txn is aborted (no Accept quorum).
    // Verify: no phantom write, no stuck transaction.
    todo!("requires live 3-node cluster")
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

/// C6.6: SIGSTOP a node for 30s, SIGCONT; the Accord state machine converges.
/// No phantom writes, no stuck transactions.
#[tokio::test]
async fn pause_resume_state_convergence() {
    if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
    {
        panic!(
            "pause_resume_state_convergence requires a live 3-node cluster — \
             set FERROSA_TEST_CLUSTER_NODES or run scripts/lima-fc-cluster-up.sh \
             and set FERROSA_TEST_FIRECRACKER=1"
        );
    }
    // 3 nodes. SIGSTOP node 2 for 30s.
    // Continue writing to nodes 1 and 3 (quorum = 2).
    // SIGCONT node 2.
    // Wait for convergence (all nodes agree on all committed txns).
    // Assert: no phantom writes; every committed write is readable from all nodes.
    todo!("requires live 3-node cluster with SIGSTOP/SIGCONT capability")
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
