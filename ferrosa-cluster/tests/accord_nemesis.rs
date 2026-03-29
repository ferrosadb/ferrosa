//! Accord nemesis stub tests.
//!
//! The full nemesis implementations (disk failure, packet reorder, LWT batch
//! atomicity) live in `ferrosa-jepsen/tests/nemesis_correctness.rs` where
//! they have access to the full Jepsen test harness.
//!
//! These stubs:
//! 1. Are discoverable by `cargo test -p ferrosa-cluster` for CI visibility.
//! 2. Skip gracefully when no cluster environment is configured.
//! 3. Point to the authoritative test location so engineers know where to look.
//!
//! To run the full tests:
//!   FERROSA_TEST_CLUSTER_NODES=host:port \
//!     cargo test -p ferrosa-jepsen --test nemesis_correctness

/// Skip helper — returns true when neither cluster env var is set.
fn should_skip() -> bool {
    std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
        && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
}

/// Disk failure nemesis: no phantom commits after disk error + recovery.
///
/// Full test: `ferrosa-jepsen/tests/nemesis_correctness.rs::disk_fail_no_phantom_commits`
#[tokio::test]
async fn disk_fail_no_phantom_commits() {
    if should_skip() {
        eprintln!(
            "skip: full test in ferrosa-jepsen/tests/nemesis_correctness.rs; \
             set FERROSA_TEST_CLUSTER_NODES or FERROSA_TEST_FIRECRACKER=1 to run"
        );
        return;
    }
    // When cluster env IS set, defer to the ferrosa-jepsen test runner.
    // Run: cargo test -p ferrosa-jepsen --test nemesis_correctness -- disk_fail_no_phantom_commits
    eprintln!("cluster env detected — run this test via ferrosa-jepsen for full coverage");
}

/// Packet reorder nemesis: all operations remain linearizable under reordering.
///
/// Full test: `ferrosa-jepsen/tests/nemesis_correctness.rs::packet_reorder_linearizability`
#[tokio::test]
async fn packet_reorder_linearizability() {
    if should_skip() {
        eprintln!(
            "skip: full test in ferrosa-jepsen/tests/nemesis_correctness.rs; \
             set FERROSA_TEST_CLUSTER_NODES or FERROSA_TEST_FIRECRACKER=1 to run"
        );
        return;
    }
    eprintln!("cluster env detected — run this test via ferrosa-jepsen for full coverage");
}

/// LWT batch atomicity under all phase-1 nemeses.
///
/// Full test: `ferrosa-jepsen/tests/nemesis_correctness.rs::lwt_batch_atomicity_all_nemeses`
#[tokio::test]
async fn lwt_batch_atomicity_all_nemeses() {
    if should_skip() {
        eprintln!(
            "skip: full test in ferrosa-jepsen/tests/nemesis_correctness.rs; \
             set FERROSA_TEST_CLUSTER_NODES or FERROSA_TEST_FIRECRACKER=1 to run"
        );
        return;
    }
    eprintln!("cluster env detected — run this test via ferrosa-jepsen for full coverage");
}
