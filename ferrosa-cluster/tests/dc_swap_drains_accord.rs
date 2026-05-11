//! Sprint 7 W7.8 — `MembershipChanger::swap_dc` drains Accord.
//!
//! In a 3+3 dual-DC topology with active Accord traffic, swapping
//! DC1 → DC3 must wait for every in-flight Accord transaction
//! referencing a leaving voter to complete or abort before the
//! joint config commits. (I-30.)
//!
//! The drain is exercised through an `AccordDrainQuery` trait
//! object: production wires it to the Accord coordinator pool,
//! tests use a deterministic stub.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::membership::{AccordDrainQuery, MembershipChanger, SwapDcOutcome};
use ferrosa_common::{AccordTimestamp, TxnId};

/// Stub drain query that simulates `n_inflight` in-flight transactions
/// then drops one in-flight txn per `tick()`.
#[derive(Debug)]
struct StubDrain {
    /// Number of in-flight txns reported on each call.
    pending: AtomicU64,
}

impl StubDrain {
    fn new(n_inflight: u64) -> Arc<Self> {
        Arc::new(Self {
            pending: AtomicU64::new(n_inflight),
        })
    }

    fn tick(&self) {
        let prev = self.pending.load(Ordering::Relaxed);
        if prev > 0 {
            self.pending.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl AccordDrainQuery for StubDrain {
    fn inflight_for_voters(&self, _voters: &[u64]) -> Vec<TxnId> {
        let n = self.pending.load(Ordering::Relaxed);
        // Synthetic txn ids for the stub.
        (0..n)
            .map(|i| TxnId::new(0, AccordTimestamp::synthetic(1 + i)))
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dc_swap_drains_accord_completion() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("leader");

    let raft = cluster.leader_node().raft.clone();
    let net = cluster.membership_network();
    let changer = MembershipChanger::for_dc("dc1", raft, net);

    // Two in-flight Accord txns referencing the leaving DC's voters.
    let drain = StubDrain::new(2);
    // Drive the drain to zero in a background task — completes after
    // 200ms total, within the swap_dc 60s default timeout.
    let drain_drive = drain.clone();
    let drain_task = tokio::spawn(async move {
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(60)).await;
            drain_drive.tick();
        }
    });

    let leaving_voter_ids = cluster
        .nodes()
        .iter()
        .map(|n| n.node_id)
        .collect::<Vec<u64>>();

    // swap_dc: drain succeeds within the deadline; outcome must be
    // `Drained` (not `TimedOut`).
    let outcome = changer
        .swap_dc(
            &leaving_voter_ids,
            drain.as_ref(),
            Duration::from_secs(2),
            Duration::from_millis(20),
        )
        .await
        .expect("drain should succeed");
    drain_task.await.unwrap();

    assert!(
        matches!(outcome, SwapDcOutcome::Drained { .. }),
        "drain that completes within deadline must report Drained; got {outcome:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dc_swap_drains_accord_timeout() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("leader");

    let raft = cluster.leader_node().raft.clone();
    let net = cluster.membership_network();
    let changer = MembershipChanger::for_dc("dc1", raft, net);

    // One stuck txn that never completes — drive timeout path.
    let drain = StubDrain::new(1);
    let leaving = vec![1u64];
    let outcome = changer
        .swap_dc(
            &leaving,
            drain.as_ref(),
            Duration::from_millis(150),
            Duration::from_millis(20),
        )
        .await
        .expect("swap_dc must surface timeout via outcome, not error");

    assert!(
        matches!(outcome, SwapDcOutcome::TimedOut { remaining } if remaining == 1),
        "stuck drain must report TimedOut with remaining=1; got {outcome:?}"
    );

    cluster.shutdown().await;
}
