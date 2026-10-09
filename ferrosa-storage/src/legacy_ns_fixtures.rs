//! Test fixtures for data written before t_cf637b6e: Accord stamped cells,
//! primary-key liveness and deletion markers with the HLC's NANOSECONDS while
//! every other write used microseconds.
//!
//! #532 was tested only against rows written by the new code, and broke every
//! CAS on rows written by the old code. These fixtures build the OLD state the
//! way the old binary left it on disk, without going through any path the fix
//! changes:
//!
//! - [`write_raw_sstable`] writes an SSTable with the real
//!   [`SSTableWriter`](ferrosa_sstable::writer::SSTableWriter) straight into a
//!   table directory, so its Data.db and Statistics.db hold the raw values (a
//!   legacy file's header minimum is >= 1e18). The engine must be reopened to
//!   load it, exactly as an upgraded node does.
//! - A commit-log segment written by the old binary is produced by
//!   `StorageEngine::write` with nanosecond stamps followed by a restart: the
//!   commit log is appended before the memtable normalises the row.

#![cfg(any(test, feature = "test-support"))]

use std::path::Path;
use std::sync::Arc;

use ferrosa_common::schema::TableSchema;
use ferrosa_common::{CellValue, DecoratedKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use crate::engine::{StorageEngine, StorageEngineConfig};
use crate::flush::{FileFlushTarget, FlushTarget};
use crate::TableId;

/// The smallest raw stamp a legacy (nanosecond) file holds. Spelled out
/// rather than imported so these fixtures compile against a pre-fix build.
const LEGACY_NS_THRESHOLD: i64 = 1_000_000_000_000_000_000;

/// Wall-clock microseconds since the epoch, as a CQL write stamps cells.
pub fn wall_now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_micros() as i64
}

/// The nanosecond stamp the pre-fix Accord apply wrote for an LWT agreed at
/// `micros` (the HLC's `t.time`), with sub-microsecond digits a real HLC has.
pub fn legacy_ns(micros: i64) -> i64 {
    micros * 1_000 + 789
}

/// One clustering-less row with cell 0 = `value`, every LWW stamp at `ts`.
pub fn row_at(value: &[u8], ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Write `partitions` (token-sorted) as one SSTable generation in
/// `table_dir`, with every timestamp exactly as given — no normalisation.
/// Returns the generation. Reopen the engine to load it.
pub fn write_raw_sstable(
    table_dir: &Path,
    schema: &TableSchema,
    partitions: &[Partition],
) -> ferrosa_common::Result<u64> {
    let header = crate::flush::header_for_flush(schema, partitions);
    let options = crate::engine::write_options_for_schema(schema, true)?;
    let mut writer = ferrosa_sstable::writer::SSTableWriter::new(options, header);
    for partition in partitions {
        writer.add_partition(partition)?;
    }
    let output = writer.finish()?;
    let target = FileFlushTarget::new_starting_at(table_dir.to_path_buf())?;
    target.flush(output)?;
    Ok(target.last_generation())
}

/// Open (or reopen) an engine at `dir` the way a restarted node does —
/// [`StorageEngine::open`] replays the commit log — and register `schema`.
/// Background compaction is held off so only explicit compactions run.
pub fn open_engine(dir: &Path, schema: &TableSchema) -> Arc<StorageEngine> {
    let mut config = StorageEngineConfig::test_config(dir);
    config.compaction.min_threshold = 50;
    let (engine, pending) = StorageEngine::open(config, None).expect("open engine");
    assert!(pending.is_empty(), "replay must apply in place");
    let engine = Arc::new(engine);
    engine
        .register_table(schema.clone())
        .expect("register table");
    engine
}

/// How the legacy rows reached this build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacySeed {
    /// Written in-process with nanosecond stamps, as the old Accord apply did.
    Memtable,
    /// Written by the old binary, unflushed, replayed from its commit log.
    CommitLogReplay,
    /// Flushed by the old binary: a raw SSTable, header minimum >= 1e18.
    Sstable,
}

impl LegacySeed {
    /// Every seed, for tests that must hold for all of them.
    pub const ALL: [LegacySeed; 3] = [
        LegacySeed::Memtable,
        LegacySeed::CommitLogReplay,
        LegacySeed::Sstable,
    ];
}

/// An engine at `dir` holding `partitions` (key -> rows) for `schema`'s table
/// as the old build left them. Asserts the seed took: the SSTable's stored
/// header minimum is >= 1e18, and replayed or flushed rows are readable.
pub fn seed_legacy(
    dir: &Path,
    schema: &TableSchema,
    seed: LegacySeed,
    partitions: Vec<(DecoratedKey, Vec<Row>)>,
) -> Arc<StorageEngine> {
    let tid = TableId::new(&schema.keyspace, &schema.table);
    let keys: Vec<DecoratedKey> = partitions.iter().map(|(k, _)| k.clone()).collect();
    let write_all = |engine: &StorageEngine, partitions: Vec<(DecoratedKey, Vec<Row>)>| {
        for (key, rows) in partitions {
            for row in rows {
                let ts = row.primary_key_liveness.timestamp;
                engine.write(&tid, &key, row, ts).expect("seed write");
            }
        }
    };
    let engine = match seed {
        LegacySeed::Memtable => {
            let engine = open_engine(dir, schema);
            write_all(&engine, partitions);
            engine
        }
        LegacySeed::CommitLogReplay => {
            {
                let engine = open_engine(dir, schema);
                // Persist the schema so the restart replays into the table.
                engine.flush(&tid).expect("flush schema");
                write_all(&engine, partitions);
                engine.force_commit_log_sync().expect("sync commit log");
                assert_eq!(engine.sstable_count(&tid), 0, "seed rows stay unflushed");
            }
            open_engine(dir, schema)
        }
        LegacySeed::Sstable => {
            let table_dir = open_engine(dir, schema).table_sstable_dir(&tid);
            let mut parts: Vec<Partition> = partitions
                .into_iter()
                .map(|(key, rows)| Partition {
                    key,
                    deletion: DeletionTime::LIVE,
                    static_row: None,
                    rows,
                })
                .collect();
            parts.sort_by(|a, b| a.key.cmp(&b.key));
            let gen = write_raw_sstable(&table_dir, schema, &parts).expect("write raw SSTable");
            let raw = stored_header(&table_dir, gen).expect("read stored header");
            assert!(
                raw.min_timestamp >= LEGACY_NS_THRESHOLD,
                "seed must be a legacy file: stored header min {} < 1e18",
                raw.min_timestamp
            );
            let engine = open_engine(dir, schema);
            assert_eq!(engine.sstable_count(&tid), 1, "the legacy SSTable loads");
            engine
        }
    };
    for key in &keys {
        assert!(
            engine.read(&tid, key).expect("read seed").is_some(),
            "{seed:?}: seeded partition {:?} is readable",
            key.key.as_bytes()
        );
    }
    engine
}

/// The Statistics.db header of generation `gen` in `table_dir` as stored,
/// read from the file rather than through any reader the fix touches.
pub fn stored_header(
    table_dir: &Path,
    gen: u64,
) -> ferrosa_common::Result<ferrosa_sstable::statistics::SerializationHeader> {
    let bytes = std::fs::read(table_dir.join(format!("{gen}-Statistics.db")))?;
    Ok(ferrosa_sstable::statistics::read_statistics(&bytes)?.header)
}
