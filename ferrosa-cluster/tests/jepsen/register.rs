//! Jepsen register tests: concurrent reads/writes to a single CQL row.
//!
//! These tests verify linearizability of a single register (one CQL row)
//! under various nemesis conditions.

use super::infrastructure::*;

// ---------------------------------------------------------------------------
// A5.2: Register tests (3 tests)
// ---------------------------------------------------------------------------

// A5.2-1: jepsen_register_linearizability
#[test]
fn jepsen_register_linearizability() {
    let mut cluster = JepsenCluster::new(3);

    // Create 3 clients, each targeting a different node.
    let mut client1 = CqlClient::new(1, 1);
    let mut client2 = CqlClient::new(2, 2);
    let mut client3 = CqlClient::new(3, 3);

    let key = b"register";

    // Phase 1: Sequential writes — establish a baseline.
    let (_txn1, committed1) = cluster.execute_write(&mut client1, key, 1);
    assert!(committed1, "write 1 should commit on healthy cluster");

    let (_txn2, committed2) = cluster.execute_write(&mut client2, key, 2);
    assert!(committed2, "write 2 should commit on healthy cluster");

    let (_txn3, committed3) = cluster.execute_write(&mut client3, key, 3);
    assert!(committed3, "write 3 should commit on healthy cluster");

    // Phase 2: Read from each node — should all see value 3 (last write).
    let val1 = cluster.execute_read(&client1);
    let val2 = cluster.execute_read(&client2);
    let val3 = cluster.execute_read(&client3);

    assert_eq!(val1, Some(3), "client1 should read last written value");
    assert_eq!(val2, Some(3), "client2 should read last written value");
    assert_eq!(val3, Some(3), "client3 should read last written value");

    // Phase 3: Interleaved writes and reads.
    let (_, committed4) = cluster.execute_write(&mut client1, key, 10);
    assert!(committed4);

    let val_after = cluster.execute_read(&client2);
    assert_eq!(val_after, Some(10), "should read latest write");

    let (_, committed5) = cluster.execute_write(&mut client3, key, 20);
    assert!(committed5);

    let val_final = cluster.execute_read(&client1);
    assert_eq!(val_final, Some(20), "should read latest write");

    // Phase 4: Verify linearizability of the entire history.
    let result = LinearizabilityChecker::check(cluster.recorder.history());
    assert!(
        result.is_ok(),
        "history should be linearizable: {:?}",
        result.err()
    );
}

// A5.2-2: jepsen_register_with_partition
#[test]
fn jepsen_register_with_partition() {
    let mut cluster = JepsenCluster::new(3);

    let mut client1 = CqlClient::new(1, 1);
    let mut client2 = CqlClient::new(2, 2);
    let client3 = CqlClient::new(3, 3);

    let key = b"register";

    // Write an initial value.
    let (_, committed) = cluster.execute_write(&mut client1, key, 100);
    assert!(committed, "initial write should succeed");

    // Verify all nodes see the value.
    assert_eq!(cluster.execute_read(&client1), Some(100));
    assert_eq!(cluster.execute_read(&client2), Some(100));
    assert_eq!(cluster.execute_read(&client3), Some(100));

    // Introduce a network partition: {1} vs {2, 3}.
    cluster.nemesis.inject(NemesisType::Partition {
        side_a: vec![1],
        side_b: vec![2, 3],
    });

    // Write from node 1 (minority partition) should fail — can't reach quorum.
    let (_, committed_partitioned) = cluster.execute_write(&mut client1, key, 999);
    assert!(
        !committed_partitioned,
        "write from minority partition should fail"
    );

    // Write from node 2 (majority partition) should succeed.
    // Node 2 can reach node 3, giving it a quorum of 2/3.
    let (_, committed_majority) = cluster.execute_write(&mut client2, key, 200);
    assert!(
        committed_majority,
        "write from majority partition should succeed"
    );

    // Reads from the majority side should see 200.
    assert_eq!(cluster.execute_read(&client2), Some(200));
    assert_eq!(cluster.execute_read(&client3), Some(200));

    // Node 1 can't reach a quorum, so a linearizable read fails.
    // This is correct: minority partition nodes must reject reads to
    // prevent stale reads, which is the whole point of linearizability.
    assert_eq!(
        cluster.execute_read(&client1),
        None,
        "minority partition node should fail linearizable read"
    );

    // Heal the partition.
    cluster.nemesis.heal_partitions();

    // After healing, write from node 1 should succeed again.
    let (_, committed_healed) = cluster.execute_write(&mut client1, key, 300);
    assert!(
        committed_healed,
        "write should succeed after partition heals"
    );

    // All nodes should converge to 300.
    assert_eq!(cluster.execute_read(&client1), Some(300));
    assert_eq!(cluster.execute_read(&client2), Some(300));
    assert_eq!(cluster.execute_read(&client3), Some(300));

    // Verify linearizability of the entire history.
    // Failed ops (minority read, minority write) are excluded by the checker.
    let result = LinearizabilityChecker::check(cluster.recorder.history());
    assert!(
        result.is_ok(),
        "history should be linearizable after partition heals: {:?}",
        result.err()
    );
}

// A5.2-3: jepsen_register_with_clock_skew
#[test]
fn jepsen_register_with_clock_skew() {
    let mut cluster = JepsenCluster::new(3);

    let mut client1 = CqlClient::new(1, 1);
    let mut client2 = CqlClient::new(2, 2);
    let mut client3 = CqlClient::new(3, 3);

    let key = b"register";

    // Write an initial value with no skew.
    let (_, committed) = cluster.execute_write(&mut client1, key, 1);
    assert!(committed);

    // Inject clock skew: node 2's clock is +500ms ahead.
    cluster.nemesis.inject(NemesisType::ClockSkew {
        node_id: 2,
        offset_us: 500_000,
    });

    // Write from skewed node 2. The timestamp will be higher due to
    // the clock offset, but the Accord protocol should still produce
    // a valid linearizable ordering.
    let (_, committed_skewed) = cluster.execute_write(&mut client2, key, 2);
    assert!(
        committed_skewed,
        "write with clock skew should still commit"
    );

    // Read from all nodes — should see value 2.
    assert_eq!(cluster.execute_read(&client1), Some(2));
    assert_eq!(cluster.execute_read(&client2), Some(2));
    assert_eq!(cluster.execute_read(&client3), Some(2));

    // Write from node 3 with no skew — should still work.
    let (_, committed3) = cluster.execute_write(&mut client3, key, 3);
    assert!(committed3);

    // All nodes converge.
    assert_eq!(cluster.execute_read(&client1), Some(3));
    assert_eq!(cluster.execute_read(&client2), Some(3));
    assert_eq!(cluster.execute_read(&client3), Some(3));

    // Inject negative skew on node 1 (clock behind).
    cluster.nemesis.inject(NemesisType::ClockSkew {
        node_id: 1,
        offset_us: -200_000,
    });

    // Write from behind-clock node.
    let (_, committed_behind) = cluster.execute_write(&mut client1, key, 4);
    assert!(
        committed_behind,
        "write with negative clock skew should commit"
    );

    assert_eq!(cluster.execute_read(&client1), Some(4));
    assert_eq!(cluster.execute_read(&client2), Some(4));
    assert_eq!(cluster.execute_read(&client3), Some(4));

    // Remove all nemesis effects.
    cluster.nemesis.heal_all();

    // Final write with no skew.
    let (_, committed_final) = cluster.execute_write(&mut client1, key, 5);
    assert!(committed_final);

    // Verify linearizability.
    let result = LinearizabilityChecker::check(cluster.recorder.history());
    assert!(
        result.is_ok(),
        "history should be linearizable despite clock skew: {:?}",
        result.err()
    );
}
