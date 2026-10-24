//! Replay dedup is per MUTATION, keyed on `mutation_id` alone (T1 P1, invariant I2).
//!
//! The commit log is replayed at startup through two paths that share one rule:
//! `StorageEngine::open` (the streaming pass) and [`StorageEngine::replay_mutations`]
//! (the deferred/pending pass). Each keeps a `HashSet<[u8; 16]>` of ids it has
//! already applied **for one pass, across every table**, skips any mutation whose
//! non-zero id is already in the set, and never skips a zero id (the legacy
//! sentinel: segments written before the field existed must always re-apply).
//!
//! That rule is why `Mutation` mints one id per mutation and never one id per
//! statement or batch: the id is the *only* thing dedup looks at, so two distinct
//! mutations sharing an id would collapse into the first one on replay — silently
//! losing the second. These tests pin both halves: a repeated id is applied once,
//! a zero id is applied every time, and a batch of mutations carries one distinct
//! id each.

use std::path::Path;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use crate::commitlog::mutation::Mutation;
use crate::{CommitLog, CommitLogConfig, StorageEngine, StorageEngineConfig, TableId};

const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const KEYSPACE: &str = "ks";
const TABLE: &str = "dedup";

/// `ks.dedup (pk text PRIMARY KEY, val text)`.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        key_type: UTF8.to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: UTF8.to_string(),
        }],
        extensions: Default::default(),
    }
}

fn table_id() -> TableId {
    TableId::new(KEYSPACE, TABLE)
}

fn key(pk: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(pk.as_bytes().to_vec()))
}

fn row(value: &[u8], ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// A one-row mutation with the id under test.
fn mutation(pk: &str, value: &[u8], ts: i64, mutation_id: [u8; 16]) -> Mutation {
    Mutation {
        mutation_id,
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        key: key(pk),
        rows: vec![row(value, ts)],
        timestamp: ts,
    }
}

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig::test_config(dir)
}

/// Opens `dir` and returns the engine plus the mutations replay did not apply
/// during open (their tables are not in a persisted schema snapshot, so the
/// open-time pass hands them to the caller instead of dropping them).
fn open_pending(dir: &Path) -> (StorageEngine, Vec<Mutation>) {
    StorageEngine::open(engine_config(dir), None).expect("open over the segment")
}

fn read_value(engine: &StorageEngine, pk: &str) -> Option<Vec<u8>> {
    engine
        .read(&table_id(), &key(pk))
        .expect("read the partition")
        .and_then(|partition| {
            partition
                .rows
                .iter()
                .flat_map(|row| row.cells.iter())
                .find(|(idx, _)| *idx == 0)
                .and_then(|(_, cell)| cell.value.clone())
        })
}

/// A segment read twice applies each mutation once.
///
/// The same id appears twice in the log — what a crash during a previous replay
/// leaves behind, because the retained copy is re-logged and both copies remain.
/// Each pair carries a DIFFERENT payload so that applying the second one would be
/// visible in the read: a higher-timestamp cell would win. Dedup applies the first
/// copy only, which is the whole point — and it is what would fail if a statement's
/// mutations ever shared one id, since only the first mutation of the batch would
/// survive.
#[test]
fn a_segment_read_twice_applies_each_mutation_once() {
    let dir = tempfile::tempdir().unwrap();
    let ids: Vec<[u8; 16]> = (0u8..3).map(|i| [0x10 + i; 16]).collect();

    {
        let log = CommitLog::new(CommitLogConfig::test_config(dir.path())).unwrap();
        for (i, id) in ids.iter().enumerate() {
            let pk = format!("pk{i}");
            // First copy, then the retained duplicate with a newer payload.
            log.append(&mutation(&pk, b"first", 100, *id)).unwrap();
            log.append(&mutation(&pk, b"second", 200, *id)).unwrap();
        }
        log.shutdown().unwrap();
    }

    let (engine, pending) = open_pending(dir.path());
    assert_eq!(
        pending.len(),
        ids.len(),
        "open-time replay must pass each mutation id through once, not once per copy"
    );

    engine.register_table(schema()).unwrap();
    // Replay the segment content a second time: dedup is per pass, so this is the
    // same mutations arriving again, and the result must be identical.
    engine.replay_mutations(pending.clone()).unwrap();
    engine.replay_mutations(pending).unwrap();

    for (i, _) in ids.iter().enumerate() {
        let pk = format!("pk{i}");
        assert_eq!(
            read_value(&engine, &pk).as_deref(),
            Some(b"first".as_slice()),
            "{pk}: the duplicate carrying the newer value must have been skipped"
        );
    }
    engine.shutdown().unwrap();
}

/// The legacy zero id is the "never deduplicate" sentinel: every copy is applied,
/// so the newest write wins as it would on any normal write.
#[test]
fn the_legacy_zero_id_is_always_reapplied() {
    let dir = tempfile::tempdir().unwrap();

    {
        let log = CommitLog::new(CommitLogConfig::test_config(dir.path())).unwrap();
        log.append(&mutation("pk", b"first", 100, [0u8; 16]))
            .unwrap();
        log.append(&mutation("pk", b"second", 200, [0u8; 16]))
            .unwrap();
        log.shutdown().unwrap();
    }

    let (engine, pending) = open_pending(dir.path());
    assert_eq!(
        pending.len(),
        2,
        "zero ids are never deduplicated, so both copies reach the caller"
    );

    engine.register_table(schema()).unwrap();
    engine.replay_mutations(pending.clone()).unwrap();
    engine.replay_mutations(pending).unwrap();

    assert_eq!(
        read_value(&engine, "pk").as_deref(),
        Some(b"second".as_slice()),
        "the later zero-id write must be applied, not deduplicated away"
    );
    engine.shutdown().unwrap();
}

/// The same rule at the pending seam: a caller that hands
/// [`StorageEngine::replay_mutations`] a list containing one id twice gets the
/// first mutation applied and the second skipped.
#[test]
fn replay_mutations_dedups_a_repeated_id_from_its_caller() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
    engine.register_table(schema()).unwrap();

    let id = [0x33u8; 16];
    engine
        .replay_mutations(vec![
            mutation("pk", b"first", 100, id),
            mutation("pk", b"second", 200, id),
        ])
        .unwrap();

    assert_eq!(
        read_value(&engine, "pk").as_deref(),
        Some(b"first".as_slice()),
        "the second mutation under an already-applied id must be skipped"
    );
    engine.shutdown().unwrap();
}

/// The P1 decision, at the write path that owns batching: `batch_write` mints one
/// id per mutation, not one id shared by the batch. Sharing would make replay drop
/// all but the batch's first mutation (see the dedup above), so this test fails if
/// that ever changes.
#[test]
fn a_batch_mints_a_distinct_id_per_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(DecoratedKey, Row, i64)> = (0u8..3)
        .map(|i| {
            (
                key(&format!("pk{i}")),
                row(format!("v{i}").as_bytes(), 100 + i64::from(i)),
                100 + i64::from(i),
            )
        })
        .collect();

    {
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(schema()).unwrap();
        engine.batch_write(&table_id(), rows).unwrap();
        engine.commit_log.shutdown().unwrap();
    }

    let (engine, pending) = open_pending(dir.path());
    assert_eq!(pending.len(), 3, "every row of the batch reaches the log");
    let ids: Vec<[u8; 16]> = pending.iter().map(|m| m.mutation_id).collect();
    assert!(
        ids.iter().all(|id| *id != [0u8; 16]),
        "a fresh mutation must not carry the zero (legacy) sentinel"
    );
    let distinct: std::collections::HashSet<[u8; 16]> = ids.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        ids.len(),
        "each mutation of a batch needs its own id: replay dedup keys on the id alone, so a \
         shared id would apply only the batch's first mutation — the rest are lost. Ids: {ids:?}"
    );
    engine.shutdown().unwrap();
}
