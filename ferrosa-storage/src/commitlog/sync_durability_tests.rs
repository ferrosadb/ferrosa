//! P0-6 (t_88479cda): a write is acknowledged only while sync keeps up.
//!
//! Before: when the periodic sync thread died, writes were still acknowledged
//! and never fsynced; a Batch fsync error and a Group flush error were logged
//! and the write acknowledged anyway; a Group stall panicked the writer.

use super::*;

use std::sync::atomic::AtomicUsize;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use crate::commitlog::mutation::Mutation;

fn simple_mutation() -> Mutation {
    Mutation {
        mutation_id: [0x10u8; 16],
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
        rows: vec![Row {
            clustering: vec![1, 2, 3],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
        timestamp: 42_000,
    }
}

fn write_mutation(dir: &std::path::Path) -> (Arc<Segment>, u64) {
    let segment = Arc::new(Segment::new(1, 4096, dir));
    let m = simple_mutation();
    let offset = segment.allocate(Segment::entry_total_size(&m)).unwrap();
    segment.write_entry(offset, &m);
    (segment, offset)
}

/// Wait for `condition`, yielding, for at most `limit`. Panics past it.
fn wait_until(what: &str, limit: Duration, mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < limit,
            "timed out after {limit:?} waiting for {what}"
        );
        thread::yield_now();
    }
}

fn assert_not_durable(result: ferrosa_common::Result<()>, why: &str) {
    match result {
        Err(ferrosa_common::Error::CommitLogNotDurable { reason }) => {
            assert!(reason.contains(why), "refusal should say {why:?}: {reason}")
        }
        other => panic!("the write must be refused as not durable ({why}), got {other:?}"),
    }
}

fn batch_with_deadline(deadline: Duration) -> CommitLogBatchConfig {
    CommitLogBatchConfig {
        target_bytes: CommitLogBatchConfig::DEFAULT_TARGET_BYTES,
        max_delay: Duration::from_millis(1),
        sync_stall_deadline: deadline,
    }
}

#[test]
fn periodic_refuses_writes_once_its_sync_thread_panics() {
    let dir = tempfile::tempdir().unwrap();
    let (segment, offset) = write_mutation(dir.path());
    let sync = PeriodicSync::with_batch(
        Duration::from_millis(5),
        batch_with_deadline(Duration::from_secs(3600)),
        Arc::new(|| Ok(())),
    );
    sync.start().unwrap();

    sync.inject_panic();
    // This write wakes the thread, which panics at its sync attempt.
    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .expect("the thread was alive when this write arrived");
    wait_until("the sync thread to die", Duration::from_secs(10), || {
        sync.health().dead
    });

    assert_not_durable(
        sync.on_write(&segment, offset, 128, AckPolicy::Durable),
        "sync thread died",
    );
    let health = sync.health();
    assert_eq!(health.panics, 1);
    assert!(
        health
            .last_failure
            .as_deref()
            .is_some_and(|f| f.contains("injected commit-log sync panic")),
        "{health:?}"
    );
    assert!(
        sync.on_write(&segment, offset, 128, AckPolicy::CallerSyncs)
            .is_ok(),
        "a caller that fsyncs itself is not refused"
    );
}

#[test]
fn restart_replaces_a_dead_periodic_thread_and_syncs_what_it_left() {
    let dir = tempfile::tempdir().unwrap();
    let (segment, offset) = write_mutation(dir.path());
    let flushes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&flushes);
    let sync = PeriodicSync::with_batch(
        Duration::from_millis(5),
        batch_with_deadline(Duration::from_secs(3600)),
        Arc::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );
    sync.start().unwrap();
    assert!(!sync.restart().unwrap(), "a live thread is not restarted");

    sync.inject_panic();
    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .unwrap();
    wait_until("the sync thread to die", Duration::from_secs(10), || {
        sync.health().dead
    });
    assert_eq!(flushes.load(Ordering::SeqCst), 0, "it died before flushing");
    assert!(
        sync.health().unsynced_for.is_some(),
        "the write is unsynced"
    );

    assert!(sync.restart().unwrap(), "a dead thread is restarted");
    // The acknowledged write's pending count was consumed by the dead thread;
    // the new one must still sync it without a new write arriving.
    wait_until(
        "the restarted thread to sync",
        Duration::from_secs(10),
        || sync.health().unsynced_for.is_none(),
    );
    assert!(flushes.load(Ordering::SeqCst) >= 1);
    let health = sync.health();
    assert!(!health.impaired(), "{health:?}");
    assert_eq!(health.restarts, 1);
    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .expect("acks resume after the restart");
    sync.stop();
}

#[test]
fn periodic_refuses_writes_once_the_oldest_unsynced_write_passes_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let (segment, offset) = write_mutation(dir.path());
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let gate = Mutex::new(gate);
    let sync = PeriodicSync::with_batch(
        Duration::from_millis(5),
        batch_with_deadline(Duration::from_millis(30)),
        Arc::new(move || {
            // A wedged fsync: the first returns when the test releases it,
            // later ones at once (the sender is dropped by then). Bounded, so
            // a failed assertion (which drops `sync`, joining this thread
            // before `release` is dropped) fails instead of hanging.
            let _ = gate.lock().recv_timeout(Duration::from_secs(30));
            Ok(())
        }),
    );
    sync.start().unwrap();

    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .expect("the first write is inside the deadline");
    wait_until(
        "the stall deadline to pass",
        Duration::from_secs(10),
        || sync.health().stalled(),
    );
    assert!(!sync.health().dead, "a stall is not a death");
    assert_not_durable(
        sync.on_write(&segment, offset, 128, AckPolicy::Durable),
        "past the 30ms stall deadline",
    );

    release.send(()).unwrap();
    drop(release);
    wait_until("the stall to clear", Duration::from_secs(10), || {
        !sync.health().impaired()
    });
    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .expect("acks resume once fsync catches up");
    sync.stop();
}

#[test]
fn the_stall_bound_is_measured_from_the_oldest_unsynced_write() {
    let health = SyncHealth::new(Duration::from_secs(2));
    let t0 = Instant::now();
    health.note_write(t0);
    health.note_write(t0 + Duration::from_millis(1500));
    assert!(health.admit(t0 + Duration::from_millis(1999)).is_ok());
    assert!(
        health.admit(t0 + Duration::from_secs(2)).is_err(),
        "the oldest write, not the newest, sets the bound"
    );

    // A sync that covers the first writes but not a later one restarts the
    // clock at the ticket, never later.
    let ticket = health.begin_sync(t0 + Duration::from_millis(1900));
    health.note_write(t0 + Duration::from_millis(1950));
    health.sync_succeeded(ticket);
    assert_eq!(
        health
            .snapshot(t0 + Duration::from_millis(2900), true)
            .unsynced_for,
        Some(Duration::from_millis(1000)),
        "measured from the ticket at 1900ms"
    );
    let ticket = health.begin_sync(t0 + Duration::from_secs(3));
    health.sync_succeeded(ticket);
    assert!(health.all_durable());
    assert_eq!(
        health
            .snapshot(t0 + Duration::from_secs(9), true)
            .unsynced_for,
        None
    );
}

#[test]
fn a_failing_periodic_fsync_refuses_writes_until_one_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let (segment, offset) = write_mutation(dir.path());
    let failing = Arc::new(AtomicBool::new(true));
    let fail = Arc::clone(&failing);
    let sync = PeriodicSync::with_batch(
        Duration::from_millis(5),
        batch_with_deadline(Duration::from_secs(3600)),
        Arc::new(move || {
            if fail.load(Ordering::SeqCst) {
                Err(ferrosa_common::Error::Io(std::io::Error::other("EIO")))
            } else {
                Ok(())
            }
        }),
    );
    sync.start().unwrap();
    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .unwrap();
    wait_until("an fsync to fail", Duration::from_secs(10), || {
        sync.health().failing
    });
    assert_not_durable(
        sync.on_write(&segment, offset, 128, AckPolicy::Durable),
        "fsync failed: I/O error: EIO",
    );

    failing.store(false, Ordering::SeqCst);
    wait_until("an fsync to succeed", Duration::from_secs(10), || {
        !sync.health().impaired()
    });
    sync.on_write(&segment, offset, 128, AckPolicy::Durable)
        .expect("acks resume once an fsync succeeds");
    sync.stop();
}

#[test]
fn a_batch_fsync_error_fails_the_write() {
    let dir = tempfile::tempdir().unwrap();
    // The segment's "directory" is a regular file, so its flush cannot
    // create the segment file.
    let not_a_dir = dir.path().join("not_a_dir");
    std::fs::write(&not_a_dir, b"").unwrap();
    let segment = Segment::new(1, 4096, &not_a_dir);
    let m = simple_mutation();
    let offset = segment.allocate(Segment::entry_total_size(&m)).unwrap();
    segment.write_entry(offset, &m);

    let sync = BatchSync::new();
    assert_not_durable(
        sync.on_write(&segment, offset, 128, AckPolicy::Durable),
        "fsync failed",
    );
    assert_eq!(sync.health().sync_failures, 1);
}

#[test]
fn a_group_writer_is_refused_when_its_fsync_fails_instead_of_acked() {
    let dir = tempfile::tempdir().unwrap();
    let (segment, _) = write_mutation(dir.path());
    let sync = GroupSync::with_batch(
        Duration::from_millis(5),
        batch_with_deadline(Duration::from_millis(50)),
        Arc::new(|| Err(ferrosa_common::Error::Io(std::io::Error::other("EIO")))),
    );
    sync.start().unwrap();
    assert_not_durable(
        sync.on_write(&segment, 0, 128, AckPolicy::Durable),
        "stall deadline (fsync failed: I/O error: EIO)",
    );
    sync.stop();
}

#[test]
fn a_group_writer_is_refused_when_the_sync_thread_dies() {
    let dir = tempfile::tempdir().unwrap();
    let (segment, _) = write_mutation(dir.path());
    let sync = GroupSync::with_batch(
        Duration::from_millis(5),
        batch_with_deadline(Duration::from_secs(3600)),
        Arc::new(|| Ok(())),
    );
    sync.start().unwrap();
    sync.inject_panic();
    // The deadline is an hour: only the death can end this wait.
    assert_not_durable(
        sync.on_write(&segment, 0, 128, AckPolicy::Durable),
        "sync thread died",
    );
    assert!(sync.restart().unwrap());
    sync.on_write(&segment, 0, 128, AckPolicy::Durable)
        .expect("acks resume after the restart");
    sync.stop();
}
