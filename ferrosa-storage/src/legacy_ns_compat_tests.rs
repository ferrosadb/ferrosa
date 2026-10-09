//! Rows written by a build that stamped Accord cells in nanoseconds, read by
//! this one (t_cf637b6e).
//!
//! Every test STARTS from data the old code left behind — a raw SSTable whose
//! header minimum is >= 1e18, a commit-log segment replayed at startup, a
//! mixed SSTable — because #532 was tested only on rows written by the new
//! code and broke every CAS on the old ones.

use std::sync::Arc;

use ferrosa_common::schema::{ColumnDefinition, TableSchema, GC_GRACE_EXTENSION};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, LEGACY_NS_THRESHOLD};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use crate::engine::StorageEngine;
use crate::legacy_ns_fixtures::{
    legacy_ns, stored_header, wall_now_us, write_raw_sstable, LegacySeed,
};
use crate::TableId;

const KS: &str = "legacy_ks";
const TABLE: &str = "legacy_t";

fn tid() -> TableId {
    TableId::new(KS, TABLE)
}

/// `(pk text, ck int, v text, PRIMARY KEY (pk, ck))`.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TABLE.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

const CK: [u8; 4] = [0, 0, 0, 1];

fn row(value: &[u8], ts: i64) -> Row {
    Row {
        clustering: CK.to_vec(),
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

fn row_delete(ts: i64, ldt: u32) -> Row {
    Row {
        clustering: CK.to_vec(),
        cells: vec![],
        deletion: DeletionTime::new(ts, ldt),
        primary_key_liveness: LivenessInfo::NONE,
    }
}

fn partition_delete(ts: i64, ldt: u32) -> Row {
    Row {
        clustering: vec![],
        cells: vec![],
        deletion: DeletionTime::new(ts, ldt),
        primary_key_liveness: LivenessInfo::NONE,
    }
}

fn now_secs() -> u32 {
    (wall_now_us() / 1_000_000) as u32
}

fn open_engine(dir: &std::path::Path, schema: TableSchema) -> Arc<StorageEngine> {
    crate::legacy_ns_fixtures::open_engine(dir, &schema)
}

type Legacy = LegacySeed;

const ALL_SEEDS: [Legacy; 3] = LegacySeed::ALL;

/// An engine at `dir` holding `rows` for `pk` as the old build left them.
fn seed_legacy(
    dir: &std::path::Path,
    schema: &TableSchema,
    seed: Legacy,
    pk: &str,
    rows: Vec<Row>,
) -> Arc<StorageEngine> {
    crate::legacy_ns_fixtures::seed_legacy(dir, schema, seed, vec![(key(pk), rows)])
}

fn read_v(engine: &StorageEngine, pk: &str) -> Option<Vec<u8>> {
    let partition = engine.read(&tid(), &key(pk)).unwrap()?;
    let row = partition.rows.iter().find(|r| r.clustering == CK)?;
    row.cells
        .iter()
        .find(|(col, _)| *col == 0)
        .and_then(|(_, c)| c.value.clone())
}

fn read_v_ts(engine: &StorageEngine, pk: &str) -> Option<i64> {
    let partition = engine.read(&tid(), &key(pk)).unwrap()?;
    let row = partition.rows.iter().find(|r| r.clustering == CK)?;
    row.cells
        .iter()
        .find(|(col, _)| *col == 0)
        .map(|(_, c)| c.timestamp)
}

/// The single compaction output generation (`test_config` writes compaction
/// output under `<data>/compaction/<table>`).
fn compaction_output(
    data_dir: &std::path::Path,
) -> ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt> {
    let mut found = Vec::new();
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.to_string_lossy().ends_with("-Data.db") {
                found.push(path);
            }
        }
    }
    assert_eq!(
        found.len(),
        1,
        "one live Data.db after compaction: {found:?}"
    );
    let path = &found[0];
    let gen = path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .trim_end_matches("-Data.db")
        .to_string();
    crate::flush::open_file_sstable(path.parent().unwrap(), &gen).unwrap()
}

async fn compact_to_one(engine: &StorageEngine) {
    engine.force_compact_all();
    for _ in 0..1500 {
        engine.poll_compactions().await;
        if engine.sstable_count(&tid()) == 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "compaction did not swap in: {} SSTables",
        engine.sstable_count(&tid())
    );
}

// ---------------------------------------------------------------------------
// Test 3: a plain write after a legacy LWT row wins.
// ---------------------------------------------------------------------------

#[test]
fn a_plain_update_beats_a_legacy_row() {
    for seed in ALL_SEEDS {
        let dir = tempfile::tempdir().unwrap();
        let t0 = wall_now_us();
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            seed,
            "k",
            vec![row(b"lwt", legacy_ns(t0))],
        );
        // A plain CQL UPDATE one millisecond later, stamped in micros.
        engine
            .write(&tid(), &key("k"), row(b"plain", t0 + 1_000), t0 + 1_000)
            .unwrap();
        assert_eq!(
            read_v(&engine, "k").as_deref(),
            Some(b"plain".as_slice()),
            "{seed:?}: a later plain write must beat a legacy LWT cell"
        );
        engine.flush(&tid()).unwrap();
        assert_eq!(
            read_v(&engine, "k").as_deref(),
            Some(b"plain".as_slice()),
            "{seed:?}: and still after flush"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 4: DELETE (row and partition level) removes a legacy row.
// ---------------------------------------------------------------------------

#[test]
fn a_row_delete_removes_a_legacy_row() {
    for seed in ALL_SEEDS {
        let dir = tempfile::tempdir().unwrap();
        let t0 = wall_now_us();
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            seed,
            "k",
            vec![row(b"lwt", legacy_ns(t0))],
        );
        engine
            .write(
                &tid(),
                &key("k"),
                row_delete(t0 + 1_000, now_secs()),
                t0 + 1_000,
            )
            .unwrap();
        assert_eq!(read_v(&engine, "k"), None, "{seed:?}: row delete");
    }
}

#[test]
fn a_partition_delete_removes_a_legacy_row() {
    for seed in ALL_SEEDS {
        let dir = tempfile::tempdir().unwrap();
        let t0 = wall_now_us();
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            seed,
            "k",
            vec![row(b"lwt", legacy_ns(t0))],
        );
        engine
            .write(
                &tid(),
                &key("k"),
                partition_delete(t0 + 1_000, now_secs()),
                t0 + 1_000,
            )
            .unwrap();
        assert_eq!(read_v(&engine, "k"), None, "{seed:?}: partition delete");
    }
}

/// A legacy DELETE (an LWT `DELETE ... IF`) must not hide a later plain write.
#[test]
fn a_later_write_beats_a_legacy_tombstone() {
    for seed in ALL_SEEDS {
        let dir = tempfile::tempdir().unwrap();
        let t0 = wall_now_us();
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            seed,
            "k",
            vec![row_delete(legacy_ns(t0), now_secs())],
        );
        engine
            .write(&tid(), &key("k"), row(b"after", t0 + 1_000), t0 + 1_000)
            .unwrap();
        assert_eq!(
            read_v(&engine, "k").as_deref(),
            Some(b"after".as_slice()),
            "{seed:?}: a write after a legacy tombstone is visible"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 10 and 15: replayed and flushed legacy stamps read as microseconds.
// ---------------------------------------------------------------------------

#[test]
fn legacy_stamps_read_back_as_microseconds() {
    for seed in ALL_SEEDS {
        let dir = tempfile::tempdir().unwrap();
        let t0 = wall_now_us();
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            seed,
            "k",
            vec![row(b"lwt", legacy_ns(t0))],
        );
        assert_eq!(read_v_ts(&engine, "k"), Some(t0), "{seed:?}: cell stamp");
        let partition = engine.read(&tid(), &key("k")).unwrap().unwrap();
        assert_eq!(
            partition.rows[0].primary_key_liveness.timestamp, t0,
            "{seed:?}: liveness stamp"
        );
    }
}

/// Test 10: the commit-log replay itself normalises (the memtable entry point
/// is not what makes the replayed row right).
#[test]
fn commit_log_replay_normalises_a_legacy_deletion_and_liveness() {
    let dir = tempfile::tempdir().unwrap();
    let t0 = wall_now_us();
    let engine = seed_legacy(
        dir.path(),
        &schema(),
        Legacy::CommitLogReplay,
        "k",
        vec![Row {
            clustering: CK.to_vec(),
            cells: vec![(0, CellValue::live(b"v".to_vec(), legacy_ns(t0)))],
            deletion: DeletionTime::new(legacy_ns(t0 - 5), now_secs()),
            primary_key_liveness: LivenessInfo::with_timestamp(legacy_ns(t0)),
        }],
    );
    let partition = engine.read(&tid(), &key("k")).unwrap().unwrap();
    let r = &partition.rows[0];
    assert_eq!(r.deletion.marked_for_delete_at, t0 - 5);
    assert_eq!(r.primary_key_liveness.timestamp, t0);
    assert_eq!(r.cells[0].1.timestamp, t0);
}

// ---------------------------------------------------------------------------
// Tests 7 and 8: compacting a mixed SSTable.
// ---------------------------------------------------------------------------

/// A mixed SSTable (legacy `a` and `d`, micros `b` one second later), a nanosecond-only
/// SSTable, and a micros overwrite of `a` and `c`. Compaction must succeed (no
/// "below output header min" abort), keep the newer value in real time, and
/// write only microsecond stamps.
#[tokio::test]
async fn compacting_mixed_and_legacy_sstables_succeeds_and_writes_micros() {
    let dir = tempfile::tempdir().unwrap();
    let t0 = wall_now_us();
    let table_dir = {
        let engine = open_engine(dir.path(), schema());
        engine.table_sstable_dir(&tid())
    };
    let part = |pk: &str, rows: Vec<Row>| Partition {
        key: key(pk),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows,
    };
    let mut mixed = vec![
        part("a", vec![row(b"a-lwt", legacy_ns(t0))]),
        part("b", vec![row(b"b-plain", t0 + 1_000_000)]),
        // Never overwritten: its normalised stamp (t0) survives into the
        // output and sits below every input's stored minimum.
        part("d", vec![row(b"d-lwt", legacy_ns(t0))]),
    ];
    mixed.sort_by(|x, y| x.key.cmp(&y.key));
    let mixed_gen = write_raw_sstable(&table_dir, &schema(), &mixed).unwrap();
    let raw = stored_header(&table_dir, mixed_gen).unwrap();
    assert!(
        raw.min_timestamp < LEGACY_NS_THRESHOLD && raw.max_timestamp >= LEGACY_NS_THRESHOLD,
        "a mixed file: stored min {} max {}",
        raw.min_timestamp,
        raw.max_timestamp
    );
    let ns_gen = write_raw_sstable(
        &table_dir,
        &schema(),
        &[part("c", vec![row(b"c-lwt", legacy_ns(t0 + 10))])],
    )
    .unwrap();
    assert!(stored_header(&table_dir, ns_gen).unwrap().min_timestamp >= LEGACY_NS_THRESHOLD);

    let engine = open_engine(dir.path(), schema());
    assert_eq!(engine.sstable_count(&tid()), 2);
    for (pk, v) in [("a", b"a-plain"), ("c", b"c-plain")] {
        engine
            .write(&tid(), &key(pk), row(v, t0 + 2_000), t0 + 2_000)
            .unwrap();
    }
    engine.flush(&tid()).unwrap();

    compact_to_one(&engine).await;

    assert_eq!(read_v(&engine, "a").as_deref(), Some(b"a-plain".as_slice()));
    assert_eq!(read_v(&engine, "b").as_deref(), Some(b"b-plain".as_slice()));
    assert_eq!(read_v(&engine, "c").as_deref(), Some(b"c-plain".as_slice()));
    assert_eq!(read_v(&engine, "d").as_deref(), Some(b"d-lwt".as_slice()));
    assert_eq!(read_v_ts(&engine, "d"), Some(t0));

    let reader = compaction_output(dir.path());
    assert!(
        reader.stored_header().min_timestamp < LEGACY_NS_THRESHOLD,
        "output header min {}",
        reader.stored_header().min_timestamp
    );
    assert_eq!(
        reader.count_legacy_ns_timestamps().unwrap(),
        0,
        "compaction output holds only microsecond stamps"
    );
}

/// The compacted rows keep their microsecond values across a restart.
#[tokio::test]
async fn a_compacted_legacy_row_reads_the_same_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let t0 = wall_now_us();
    {
        let engine = seed_legacy(
            dir.path(),
            &schema(),
            Legacy::Sstable,
            "k",
            vec![row(b"lwt", legacy_ns(t0))],
        );
        engine
            .write(&tid(), &key("other"), row(b"x", t0 + 5), t0 + 5)
            .unwrap();
        engine.flush(&tid()).unwrap();
        compact_to_one(&engine).await;
        assert_eq!(read_v_ts(&engine, "k"), Some(t0));
    }
    let engine = open_engine(dir.path(), schema());
    assert_eq!(read_v(&engine, "k").as_deref(), Some(b"lwt".as_slice()));
    assert_eq!(read_v_ts(&engine, "k"), Some(t0));
}

// ---------------------------------------------------------------------------
// Test 9: a legacy tombstone is purged after gc_grace and kept inside it.
// ---------------------------------------------------------------------------

/// Data at `t0 - 1h` and a legacy (nanosecond) row tombstone at `t0` with the
/// given local deletion time, in a legacy SSTable; a newer unflushed write
/// sits in the memtable (so the purge guard is a real microsecond value, as on
/// any live node). Returns the compaction output's partition count.
async fn purge_scenario(tombstone_ldt: u32) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let t0 = wall_now_us();
    let mut schema = schema();
    schema
        .extensions
        .insert(GC_GRACE_EXTENSION.to_string(), "3600".to_string());
    let table_dir = {
        let engine = open_engine(dir.path(), schema.clone());
        engine
            .write(
                &tid(),
                &key("dead"),
                row(b"v", t0 - 3_600_000_000),
                t0 - 3_600_000_000,
            )
            .unwrap();
        engine
            .write(
                &tid(),
                &key("live"),
                row(b"v", t0 - 3_600_000_000),
                t0 - 3_600_000_000,
            )
            .unwrap();
        engine.flush(&tid()).unwrap();
        engine.table_sstable_dir(&tid())
    };
    write_raw_sstable(
        &table_dir,
        &schema,
        &[Partition {
            key: key("dead"),
            deletion: DeletionTime::new(legacy_ns(t0), tombstone_ldt),
            static_row: None,
            rows: vec![],
        }],
    )
    .unwrap();
    let engine = open_engine(dir.path(), schema.clone());
    assert_eq!(engine.sstable_count(&tid()), 2);
    engine
        .write(
            &tid(),
            &key("late"),
            row(b"v", t0 + 1_000_000),
            t0 + 1_000_000,
        )
        .unwrap();
    compact_to_one(&engine).await;
    compaction_output(dir.path()).key_count()
}

#[tokio::test]
async fn a_legacy_tombstone_is_purged_after_gc_grace() {
    // Deleted two hours ago: past the one-hour grace.
    assert_eq!(
        purge_scenario(now_secs() - 7_200).await,
        1,
        "the legacy partition tombstone and the row it shadowed are dropped"
    );
}

#[tokio::test]
async fn a_legacy_tombstone_inside_gc_grace_is_kept() {
    assert_eq!(
        purge_scenario(now_secs()).await,
        2,
        "inside the grace period the legacy tombstone is kept"
    );
}

// ---------------------------------------------------------------------------
// Test 17: rows a #532 build stamped in micros keep their values.
// ---------------------------------------------------------------------------

#[test]
fn rows_stamped_in_microseconds_by_532_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let t0 = wall_now_us();
    let table_dir = {
        let engine = open_engine(dir.path(), schema());
        engine.table_sstable_dir(&tid())
    };
    write_raw_sstable(
        &table_dir,
        &schema(),
        &[Partition {
            key: key("k"),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row(b"micros", t0)],
        }],
    )
    .unwrap();
    let engine = open_engine(dir.path(), schema());
    assert_eq!(read_v(&engine, "k").as_deref(), Some(b"micros".as_slice()));
    assert_eq!(read_v_ts(&engine, "k"), Some(t0));
    // A plain write one microsecond EARLIER must still lose to it.
    engine
        .write(&tid(), &key("k"), row(b"older", t0 - 1), t0 - 1)
        .unwrap();
    assert_eq!(read_v(&engine, "k").as_deref(), Some(b"micros".as_slice()));
}

// ---------------------------------------------------------------------------
// Test 16: PITR includes legacy mutations written before the target.
// ---------------------------------------------------------------------------

#[test]
fn pitr_includes_a_legacy_mutation_written_before_the_target() {
    let t0 = wall_now_us();
    let legacy = crate::Mutation {
        mutation_id: [7u8; 16],
        keyspace: KS.to_string(),
        table: TABLE.to_string(),
        key: key("k"),
        rows: vec![row(b"lwt", legacy_ns(t0))],
        timestamp: legacy_ns(t0),
    };
    let mut bytes = vec![0u8; legacy.serialized_size()];
    legacy.serialize_into(&mut bytes);
    let decoded = crate::Mutation::deserialize_from(&bytes).unwrap();
    let kept = crate::restore::validation::filter_mutations_by_timestamp(vec![decoded], t0 + 1);
    assert_eq!(
        kept.len(),
        1,
        "a mutation written before the target is restored"
    );
    let excluded = crate::restore::validation::filter_mutations_by_timestamp(
        vec![crate::Mutation::deserialize_from(&bytes).unwrap()],
        t0 - 1,
    );
    assert!(excluded.is_empty(), "and one written after it is not");
}

// ---------------------------------------------------------------------------
// The counter.
// ---------------------------------------------------------------------------

#[test]
fn the_normalisation_counter_is_exported() {
    let text = crate::metrics::render_prometheus();
    for source in ["sstable", "mutation", "memtable_write"] {
        let line =
            format!("ferrosa_storage_legacy_ns_timestamps_normalised_total{{source=\"{source}\"}}");
        assert!(text.contains(&line), "missing {line}");
    }
}
