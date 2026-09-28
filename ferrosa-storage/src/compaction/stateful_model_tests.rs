//! Tests the persisted storage lifecycle against a small last-write-wins model.
//! Correctness: generated histories preserve visible values through flush,
//! compaction, cancellation, crash/restart, and real tombstone purging.
//! Last revised: 2026-09-26
//! Last changed: Added bounded state-machine and production purge readiness tests.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema, GC_GRACE_EXTENSION};
use ferrosa_common::CancelReason;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use proptest::prelude::*;

use super::cancel_harness::{cancel_now, CancelHookGuard, CancelPoint};
use crate::engine::{StorageEngine, StorageEngineConfig};
use crate::TableId;

const MODEL_KEYS: [u8; 4] = [0, 1, 2, 3];
const MAX_PROGRAM_OPS: usize = 16;
/// Hang guard, not a pacing budget: every wait below is on a completion signal
/// (hook fired, drain finished, result posted). It only turns a worker that never
/// finishes into a loud failure, so it is sized for a heavily loaded CI box.
const MODEL_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
enum Op {
    Write,
    Delete,
    Flush,
    Compact,
    Cancel,
    Crash,
    Restart,
    Read,
}

impl Op {
    fn from_code(code: u8) -> Self {
        match code % 8 {
            0 => Self::Write,
            1 => Self::Delete,
            2 => Self::Flush,
            3 => Self::Compact,
            4 => Self::Cancel,
            5 => Self::Crash,
            6 => Self::Restart,
            _ => Self::Read,
        }
    }
}

fn schema(table: &str, gc_grace_seconds: Option<&str>) -> TableSchema {
    let mut extensions = std::collections::HashMap::new();
    if let Some(seconds) = gc_grace_seconds {
        extensions.insert(GC_GRACE_EXTENSION.to_string(), seconds.to_string());
    }
    TableSchema {
        keyspace: "ks".to_string(),
        table: table.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions,
    }
}

fn test_config(dir: &Path) -> StorageEngineConfig {
    let mut config = StorageEngineConfig::test_config(dir);
    // Keep generated flush histories below the automatic compaction threshold;
    // the model invokes compaction only at explicit command boundaries.
    config.compaction.min_threshold = 50;
    config
}

fn key(id: u8) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(format!("k{id}").into_bytes()))
}

fn live_row(value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: 1i32.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

fn partition_tombstone(timestamp: i64, local_deletion_time: u32) -> Row {
    Row {
        clustering: vec![],
        cells: vec![],
        deletion: DeletionTime::new(timestamp, local_deletion_time),
        primary_key_liveness: LivenessInfo::NONE,
    }
}

fn read_value(engine: &StorageEngine, table: &TableId, id: u8) -> Option<Vec<u8>> {
    engine
        .read(table, &key(id))
        .expect("read model key")
        .and_then(|partition| {
            partition
                .rows
                .iter()
                .flat_map(|row| row.cells.iter())
                .find_map(|(_, cell)| cell.value.clone())
        })
}

fn assert_model(engine: &StorageEngine, table: &TableId, model: &HashMap<u8, Option<Vec<u8>>>) {
    for id in MODEL_KEYS {
        let expected = model.get(&id).cloned().flatten();
        assert_eq!(read_value(engine, table, id), expected, "model key {id}");
    }
}

fn write_model_value(
    engine: &StorageEngine,
    table: &TableId,
    model: &mut HashMap<u8, Option<Vec<u8>>>,
    id: u8,
    timestamp: i64,
    value: Vec<u8>,
) {
    engine
        .write(table, &key(id), live_row(&value, timestamp), timestamp)
        .expect("write model value");
    model.insert(id, Some(value));
}

fn ensure_two_sstables(
    engine: &StorageEngine,
    table: &TableId,
    model: &mut HashMap<u8, Option<Vec<u8>>>,
    timestamp: &mut i64,
) {
    while engine.sstable_count(table) < 2 {
        let value = format!("seed-{}", *timestamp).into_bytes();
        write_model_value(engine, table, model, 0, *timestamp, value);
        *timestamp += 1;
        engine.flush(table).expect("flush compaction seed");
    }
}

async fn run_compaction(engine: &Arc<StorageEngine>, table: &TableId, cancel: bool) -> bool {
    if engine.sstable_count(table) < 2 {
        return false;
    }
    let fired = Arc::new(AtomicBool::new(false));
    let scope = table.to_string();
    let guard = if cancel {
        let fired_for_hook = Arc::clone(&fired);
        let scope_for_hook = scope.clone();
        Some(CancelHookGuard::install(
            scope.clone(),
            Arc::new(move |point| {
                if point == CancelPoint::MergePartitionFirst {
                    // ST-T9e30472a: cancel from INSIDE the hook, on the worker
                    // thread, so the cancel lands at exactly this checkpoint.
                    // Cancelling from the test thread after observing `fired`
                    // raced the merge: under load the worker could finish and
                    // post a live result first, which then sat un-polled and
                    // was integrated (and discarded) by the next Compact op.
                    cancel_now(&scope_for_hook, CancelReason::Operator);
                    fired_for_hook.store(true, Ordering::SeqCst);
                }
            }),
        ))
    } else {
        None
    };
    engine.force_compact_all();
    if cancel {
        let reached_checkpoint = tokio::time::timeout(MODEL_TIMEOUT, async {
            while !fired.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(reached_checkpoint.is_ok(), "cancel hook did not fire");
        let drained = tokio::time::timeout(
            MODEL_TIMEOUT,
            engine.pause_table_compactions(table, CancelReason::Operator),
        )
        .await;
        let pause = drained
            .expect("cancelled compaction did not drain")
            .expect("cancelled compaction drain failed");
        drop(pause);
    } else {
        assert!(
            engine.await_compaction_result(MODEL_TIMEOUT),
            "compaction result did not arrive"
        );
        engine.poll_compactions().await;
    }
    drop(guard);
    if cancel {
        assert!(fired.load(Ordering::SeqCst), "cancel hook did not fire");
        assert!(
            engine.sstable_count(table) >= 2,
            "cancel retired its inputs"
        );
    } else {
        assert_eq!(engine.sstable_count(table), 1, "compaction did not swap in");
    }
    true
}

async fn run_program(program: &[u8]) {
    let dir = tempfile::tempdir().expect("model tempdir");
    let table = TableId::new("ks", "compaction_model");
    let config = test_config(dir.path());
    let engine = Arc::new(StorageEngine::new(config, None).expect("open model engine"));
    engine
        .register_table(schema("compaction_model", None))
        .unwrap();
    let mut engine = Some(engine);
    let mut model = HashMap::new();
    let mut timestamp = 1_000i64;

    // A fixed prelude guarantees each lifecycle transition runs in every case;
    // the generated tail varies their order and repeats boundary operations.
    for (step, op) in [0u8, 2, 1, 2, 3, 4, 5, 6, 7]
        .into_iter()
        .chain(program.iter().copied().map(|code| code % 8))
        .enumerate()
    {
        if engine.is_none() {
            if op == 6 {
                let config = test_config(dir.path());
                let (restarted, _) =
                    StorageEngine::open(config, None).expect("restart model engine");
                engine = Some(Arc::new(restarted));
                assert_model(engine.as_ref().unwrap(), &table, &model);
            }
            continue;
        }
        let active = engine.as_ref().expect("active model engine");
        let mut crashed = false;
        match Op::from_code(op) {
            Op::Write => {
                let id = ((step as u8).wrapping_add(op)) % 4;
                let value = format!("value-{step}-{op}").into_bytes();
                write_model_value(active, &table, &mut model, id, timestamp, value);
                timestamp += 1;
            }
            Op::Delete => {
                let id = ((step as u8).wrapping_add(op)) % 4;
                active
                    .write(
                        &table,
                        &key(id),
                        partition_tombstone(timestamp, 0),
                        timestamp,
                    )
                    .expect("delete model key");
                model.insert(id, None);
                timestamp += 1;
            }
            Op::Flush => active.flush(&table).expect("flush model table"),
            Op::Compact => {
                ensure_two_sstables(active, &table, &mut model, &mut timestamp);
                run_compaction(active, &table, false).await;
            }
            Op::Cancel => {
                ensure_two_sstables(active, &table, &mut model, &mut timestamp);
                run_compaction(active, &table, true).await;
            }
            Op::Crash => crashed = true,
            Op::Restart => {
                // The transition is meaningful only after `Crash`; otherwise
                // the current engine remains authoritative.
            }
            Op::Read => assert_model(active, &table, &model),
        }
        if crashed {
            drop(engine.take());
        } else {
            assert_model(active, &table, &model);
        }
    }
    if let Some(engine) = engine {
        assert_model(&engine, &table, &model);
        engine.shutdown().expect("shutdown model engine");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x00_27_c5_11),
        ..ProptestConfig::default()
    })]

    #[test]
    fn compaction_model_preserves_values_across_lifecycle(ops in prop::collection::vec(any::<u8>(), 0..=MAX_PROGRAM_OPS)) {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build model runtime")
            .block_on(run_program(&ops));
    }
}

#[tokio::test]
async fn purge_readiness_drops_old_tombstones_without_resurrection() {
    let purged_before = crate::metrics::compaction_purged_markers_total();
    let dir = tempfile::tempdir().expect("purge tempdir");
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.compaction.min_threshold = 50;
    let engine = Arc::new(StorageEngine::new(config, None).expect("open purge engine"));
    let table = TableId::new("ks", "purge_readiness");
    engine
        .register_table(schema("purge_readiness", Some("0")))
        .expect("register purge table");

    engine
        .write(&table, &key(0), live_row(b"old", 1_000), 1_000)
        .expect("write old value");
    for id in 1u8..4 {
        engine
            .write(&table, &key(id), live_row(b"survivor", 1_000), 1_000)
            .expect("write survivor");
    }
    engine.flush(&table).expect("flush old value");
    engine
        .write(
            &table,
            &key(0),
            partition_tombstone(2_000, 1_000_000_000),
            2_000,
        )
        .expect("delete old value");
    engine.flush(&table).expect("flush tombstone");
    assert_eq!(engine.sstable_count(&table), 2);
    assert_eq!(
        read_value(&engine, &table, 0),
        None,
        "delete must hide old data"
    );

    assert!(run_compaction(&engine, &table, false).await);
    assert!(
        crate::metrics::compaction_purged_markers_total() > purged_before,
        "production compaction did not purge the expired tombstone"
    );
    assert_eq!(
        read_value(&engine, &table, 0),
        None,
        "purge resurrected old data"
    );
    for id in 1u8..4 {
        assert_eq!(read_value(&engine, &table, id), Some(b"survivor".to_vec()));
    }

    drop(engine);
    let (restarted, _) =
        StorageEngine::open(test_config(dir.path()), None).expect("restart after purge");
    assert_eq!(
        read_value(&restarted, &table, 0),
        None,
        "restart resurrected old data"
    );
    for id in 1u8..4 {
        assert_eq!(
            read_value(&restarted, &table, id),
            Some(b"survivor".to_vec())
        );
    }
    restarted.shutdown().expect("shutdown purge engine");
}
