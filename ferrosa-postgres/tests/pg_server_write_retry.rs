//! The seeded-write helper every PG integration test writes through honours the commit
//! log's not-durable refusal: it waits for the backlog to drain and retries the identical
//! write, and it fails loudly when the backlog never drains.
//!
//! The refusal is produced without sleeps or fault injection. The stall deadline is 1 ms and
//! the sync thread holds a dirty batch open for `BATCH_WINDOW` before it fsyncs (an
//! unreachable byte target, so only the timer closes it). One write is left unsynced until
//! it is older than the deadline; the next write, issued microseconds later and well inside
//! the window, is refused. The sync thread then drains the backlog when the window closes,
//! which is what the helper waits for. In the negative case the window never closes.
//!
//! (`force_commit_log_sync` is not used to drain: it flushes the segment but does not
//! update the sync thread's health, so `unsynced_for` stays set until the thread's own next
//! sync.)

#[path = "common/pg_server.rs"]
mod pg_server;

use std::time::Duration;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_storage::{CommitLogBatchConfig, StorageEngine, SyncStrategyConfig, TableId};
use pg_server::{
    deadline_after, engine_config, row_id, seeded_row, storage_schema, wait_for_commit_log_drain,
    write_row_with,
};

const STALL_DEADLINE: Duration = Duration::from_millis(1);
const NEVER: Duration = Duration::from_secs(3600);

/// The batch window of the draining engine: long next to the microseconds between the
/// stalling write and the refused one, short enough to keep the test quick.
const BATCH_WINDOW: Duration = Duration::from_secs(3);

/// An engine whose commit log fsyncs a dirty batch only after `window`, with a 1 ms stall
/// deadline.
fn engine_syncing_after(dir: &std::path::Path, window: Duration) -> StorageEngine {
    let mut config = engine_config(dir);
    config.commit_log.sync_strategy = SyncStrategyConfig::Periodic {
        sync_interval: window,
    };
    config.commit_log.batch = CommitLogBatchConfig {
        target_bytes: u64::MAX,
        max_delay: window,
        sync_stall_deadline: STALL_DEADLINE,
        ..CommitLogBatchConfig::with_max_delay(window)
    };
    let engine = StorageEngine::new(config, None).expect("engine");
    engine.register_table(storage_schema()).expect("register");
    engine
}

/// Leave one acknowledged write unsynced until it is past the stall deadline, so the next
/// durable write is refused.
fn stall_the_commit_log(engine: &StorageEngine) {
    let tid = TableId::new("ks", "t");
    let key = DecoratedKey::new(PartitionKey::new(b"stall".to_vec()));
    engine
        .write(&tid, &key, seeded_row(0, 1), 1)
        .expect("the first write is younger than the deadline");
    let mut expired = deadline_after(Duration::from_secs(10));
    loop {
        let unsynced = engine.commit_log_sync_health().unsynced_for;
        match unsynced {
            Some(age) if age > STALL_DEADLINE => return,
            Some(_) => assert!(!expired(), "the unsynced write never aged"),
            None => panic!("the commit log synced before the write aged; the setup is not stalled"),
        }
        std::thread::yield_now();
    }
}

fn stored_scores(engine: &StorageEngine, i: usize) -> Vec<Vec<u8>> {
    let tid = TableId::new("ks", "t");
    let key = DecoratedKey::new(PartitionKey::new(row_id(i).into_bytes()));
    let partition = engine.read(&tid, &key).expect("read").expect("row present");
    partition
        .rows
        .iter()
        .map(|row| {
            let (_, score) = row
                .cells
                .iter()
                .find(|(column, _)| *column == 1)
                .expect("score");
            score
                .value
                .as_ref()
                .expect("live score")
                .as_slice()
                .to_vec()
        })
        .collect()
}

#[test]
fn a_refused_seeded_write_waits_for_the_drain_and_lands_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_syncing_after(dir.path(), BATCH_WINDOW);
    stall_the_commit_log(&engine);

    let mut drains = 0usize;
    let refusals = write_row_with(&engine, 7, &mut |engine: &StorageEngine| {
        assert!(
            engine.commit_log_sync_health().unsynced_for.is_some(),
            "refused while a backlog is pending"
        );
        wait_for_commit_log_drain(engine, deadline_after(Duration::from_secs(30)));
        assert!(
            engine.commit_log_sync_health().unsynced_for.is_none(),
            "the retry runs only after the backlog drained"
        );
        drains += 1;
    });

    assert_eq!(
        refusals.len(),
        1,
        "refused once, then retried: {refusals:?}"
    );
    assert!(
        refusals[0].contains("stall deadline"),
        "the refusal is the stall guard: {}",
        refusals[0]
    );
    assert_eq!(drains, 1, "one drain before the one retry");
    assert_eq!(
        stored_scores(&engine, 7),
        vec![7i32.to_be_bytes().to_vec()],
        "the identical row landed exactly once"
    );
}

#[test]
#[should_panic(expected = "the commit log never drained its backlog")]
fn a_backlog_that_never_drains_fails_the_seeded_write_loudly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_syncing_after(dir.path(), NEVER);
    stall_the_commit_log(&engine);

    // Nothing syncs, so the drain wait must give up. A poll budget stands in for the wall
    // clock so the test is deterministic and takes no time.
    write_row_with(&engine, 8, &mut |engine: &StorageEngine| {
        let mut polls = 0usize;
        wait_for_commit_log_drain(engine, move || {
            polls += 1;
            polls > 1_000
        });
    });
}
