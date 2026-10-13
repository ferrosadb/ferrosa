//! T-F1-4 (outage 2026-10-10, F1 / decision D1).
//!
//! A follower that falls behind a leader whose logs were purged can only
//! catch up through the leader's current snapshot. openraft builds snapshots
//! on the builder `get_snapshot_builder` hands out, in a detached task, and
//! serves followers from the live state machine's `get_current_snapshot`.
//! When the builder cached its snapshot on a private clone, the live machine
//! kept serving an older snapshot (or none) while the log store purged past
//! it, and the follower never caught up: on 2026-10-10 the leader offered
//! snapshot 47870 to a follower at 48181 about 160,000 times in 15 minutes.
//!
//! The in-process harness drives the real `FerrosStateMachine` through its
//! real `get_snapshot_builder`, so this reproduces the outage end to end.

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::raft_harness::{HarnessRaftKnobs, SnapshotSend, TestCluster};
use ferrosa_cluster::raft::{RaftCommand, RaftOp, RaftResponse};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

const ENTRIES: usize = 50;

/// Catch-up deadline after the partition heals.
///
/// Measured 2026-10-10 on an M-series laptop shared with a 6-job build: with
/// the fix the follower matched the commit index 59-278 ms after heal across
/// 10 runs (median ~190 ms). 15 s is ~50x the worst observation, leaving room
/// for a loaded CI runner while staying far below "never", which is what the
/// defect produces (red run: matched=None after the full deadline).
const CATCH_UP_DEADLINE: Duration = Duration::from_secs(15);

/// Observed 1-2 per run (2 when openraft first delivers the earlier snapshot
/// it fetched while the follower was unreachable). 3 tolerates one more retry
/// racing a heartbeat. The outage sent ~180 per second.
const MAX_SNAPSHOT_SENDS: usize = 3;

fn keyspace_command(i: usize) -> RaftCommand {
    let mut options = std::collections::HashMap::new();
    options.insert("replication_factor".to_string(), "1".to_string());
    RaftCommand {
        op: RaftOp::CreateKeyspace(KeyspaceMetadata {
            name: format!("purge_ks_{i:03}"),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options,
            },
        }),
        schema_version: uuid::Uuid::new_v4(),
    }
}

async fn propose_range(cluster: &TestCluster, range: std::ops::Range<usize>) {
    for i in range {
        let resp = cluster
            .propose_on_leader(keyspace_command(i))
            .await
            .unwrap_or_else(|e| panic!("proposal {i} failed: {e:?}"));
        assert!(
            matches!(resp, RaftResponse::Ok),
            "proposal {i} answered {resp:?}"
        );
    }
}

/// Trigger a snapshot on the leader through openraft (which uses the real
/// builder) and wait until openraft reports one at or past the leader's
/// applied index. Returns the index openraft reports it built at.
async fn trigger_snapshot(cluster: &TestCluster, leader: u64) -> u64 {
    let raft = cluster.raft_for_node_id(leader).expect("leader raft");
    let applied = raft
        .metrics()
        .borrow()
        .last_applied
        .expect("leader has applied entries")
        .index;
    raft.trigger().snapshot().await.expect("trigger snapshot");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snap = raft.metrics().borrow().snapshot.map(|id| id.index);
        if let Some(built) = snap.filter(|built| *built >= applied) {
            return built;
        }
        assert!(
            Instant::now() < deadline,
            "leader never reported a snapshot at {applied}; metrics snapshot={snap:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_purge(cluster: &TestCluster, leader: u64, upto: u64) {
    let raft = cluster.raft_for_node_id(leader).expect("leader raft");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let purged = raft.metrics().borrow().purged.map(|id| id.index);
        if purged >= Some(upto) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "leader never purged its log through {upto}; purged={purged:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_after_log_purge_via_builder_snapshot() {
    let cluster = TestCluster::with_voters_and_knobs(
        3,
        HarnessRaftKnobs {
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
        },
    )
    .await;
    // Stop tick-driven elections so the isolated follower cannot depose the
    // leader when it returns; this test is about replication, not elections.
    let leader = cluster.pin_leadership(Duration::from_secs(10)).await;
    let follower = *cluster
        .node_ids()
        .iter()
        .find(|id| **id != leader)
        .expect("a follower");

    cluster.isolate_by_node_id(follower);

    propose_range(&cluster, 0..ENTRIES / 2).await;
    let first = trigger_snapshot(&cluster, leader).await;
    propose_range(&cluster, ENTRIES / 2..ENTRIES).await;
    let second = trigger_snapshot(&cluster, leader).await;
    assert!(second > first, "second snapshot {second} <= first {first}");
    wait_for_purge(&cluster, leader, second).await;

    let leader_raft = cluster.raft_for_node_id(leader).expect("leader raft");
    let commit = leader_raft
        .metrics()
        .borrow()
        .last_applied
        .expect("leader applied")
        .index;
    let follower_before = cluster
        .raft_for_node_id(follower)
        .expect("follower raft")
        .metrics()
        .borrow()
        .last_applied
        .map(|id| id.index);
    assert!(
        follower_before < Some(second),
        "precondition: the isolated follower ({follower_before:?}) must be behind the purge point {second}"
    );

    let sends_before = cluster.snapshot_sends().len();
    cluster.heal();
    let healed_at = Instant::now();

    let matched = loop {
        let matched = leader_raft
            .metrics()
            .borrow()
            .replication
            .as_ref()
            .and_then(|r| r.get(&follower).cloned().flatten())
            .map(|id| id.index);
        if matched >= Some(commit) {
            break matched;
        }
        if healed_at.elapsed() > CATCH_UP_DEADLINE {
            let sends: Vec<SnapshotSend> = cluster.snapshot_sends()[sends_before..].to_vec();
            panic!(
                "follower {follower} never caught up: matched={matched:?}, commit={commit}, \
                 purged through {second}; {} snapshot sends after heal: {:?}",
                sends.len(),
                sends.iter().take(10).collect::<Vec<_>>()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    eprintln!(
        "T-F1-4: follower {follower} matched {matched:?} (commit {commit}) {:?} after heal",
        healed_at.elapsed()
    );

    let sends: Vec<SnapshotSend> = cluster
        .snapshot_sends()
        .into_iter()
        .skip(sends_before)
        .filter(|s| s.from == leader && s.to == follower)
        .collect();
    eprintln!("T-F1-4: snapshots built at {first} and {second}; sent after heal: {sends:?}");
    assert!(
        !sends.is_empty() && sends.len() <= MAX_SNAPSHOT_SENDS,
        "leader sent {} snapshots to the follower (want 1..={MAX_SNAPSHOT_SENDS}): {sends:?}",
        sends.len()
    );
    // openraft keeps retrying a snapshot it fetched while the follower was
    // unreachable, so the first delivery can be the earlier snapshot (`first`);
    // the defect was the leader never offering anything past it. Every offer
    // must be a snapshot the builder produced, and the last one must cover
    // the purge point.
    for send in &sends {
        assert!(
            send.last_log_index == Some(first) || send.last_log_index >= Some(second),
            "leader offered snapshot {:?}, which the builder never produced (built {first} \
             and {second}): {sends:?}",
            send.last_log_index
        );
    }
    assert!(
        sends.last().and_then(|s| s.last_log_index) >= Some(second),
        "the last snapshot offered must cover the purge point {second}: {sends:?}"
    );

    // The follower holds the content, not merely the index.
    let follower_node_state = {
        let nodes = cluster.nodes();
        let node = nodes
            .iter()
            .find(|n| n.node_id == follower)
            .expect("follower node");
        node.state_machine.clone()
    };
    let state = follower_node_state.snapshot_state().await;
    let expected: BTreeSet<String> = (0..ENTRIES).map(|i| format!("purge_ks_{i:03}")).collect();
    let got: BTreeSet<String> = state
        .keyspaces
        .keys()
        .filter(|k| k.starts_with("purge_ks_"))
        .cloned()
        .collect();
    assert_eq!(got, expected, "follower state after catch-up");

    cluster.shutdown().await;
}
