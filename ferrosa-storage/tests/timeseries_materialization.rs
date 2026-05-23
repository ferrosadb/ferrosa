use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::timeseries::{
    ConsolidationFn, LateWindowClassification, MaterializationQueue, MaterializationRequest,
    MaterializationTarget, MaterializationTaskKind, MaterializedRollup, TimeSeriesTimestampUnit,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId, TimeSeriesWasmAggregateExecutor, TimeSeriesWasmAggregateInvocation,
};

fn target() -> MaterializationTarget {
    MaterializationTarget {
        source_table: TableId::new("ks", "sensor"),
        target_table: TableId::new("ks", "sensor_10s"),
        interval: Duration::from_secs(10),
        source_columns: vec!["value".to_string()],
        functions: vec![
            ConsolidationFn::Min,
            ConsolidationFn::Max,
            ConsolidationFn::Avg,
            ConsolidationFn::Wasm {
                keyspace: "ks".to_string(),
                function_name: "custom_rollup".to_string(),
            },
        ],
        target_result_column_indices: vec![0, 1, 2, 3],
        target_timestamp_unit: TimeSeriesTimestampUnit::Micros,
    }
}

#[test]
fn materialized_rollup_encodes_target_mutation_rows() {
    let rollup = MaterializedRollup {
        target: target(),
        partition_key: b"sensor-1".to_vec(),
        window_start_ts: 1_000_000,
    };

    let mutation = rollup.encode_mutation_from_results([2.0, 8.0, 5.0, 2.5]);

    assert_eq!(mutation.keyspace, "ks");
    assert_eq!(mutation.table, "sensor_10s");
    assert_eq!(mutation.key.key.as_bytes(), b"sensor-1");
    assert_eq!(mutation.timestamp, 1_000_000);
    assert_eq!(mutation.rows.len(), 1);

    let row = &mutation.rows[0];
    assert_eq!(row.clustering, 1_000_000_i64.to_be_bytes().to_vec());
    assert_eq!(row.cells.len(), 4);

    let decoded: Vec<f64> = row
        .cells
        .iter()
        .map(|(_, cell)| {
            let bytes = cell.value.as_ref().expect("rollup cells are live values");
            f64::from_be_bytes(bytes.as_slice().try_into().unwrap())
        })
        .collect();

    assert_eq!(decoded[0], 2.0);
    assert_eq!(decoded[1], 8.0);
    assert_eq!(decoded[2], 5.0);
    assert_eq!(decoded[3], 2.5);
}

#[test]
fn materialization_queue_tracks_descriptor_snapshots_and_drops_when_full() {
    let queue = MaterializationQueue::new(1);
    let request = MaterializationRequest {
        target: target(),
        partition_key: b"sensor-1".to_vec(),
        window_start_ts: 0,
        window_end_ts: 10_000_000,
        kind: MaterializationTaskKind::FreshBoundary,
        retry_count: 0,
    };

    assert!(queue.enqueue(request.clone()).is_ok());
    assert!(queue.enqueue(request).is_err());

    let snapshot = queue.metrics().snapshot();
    assert_eq!(snapshot.enqueued, 1);
    assert_eq!(snapshot.dropped_full, 1);
    assert_eq!(snapshot.pending, 1);

    let drained = queue.drain_next().expect("queued descriptor drains");
    assert_eq!(drained.partition_key, b"sensor-1");
    assert_eq!(drained.window_start_ts, 0);
    assert_eq!(drained.window_end_ts, 10_000_000);
    assert_eq!(drained.kind, MaterializationTaskKind::FreshBoundary);

    let snapshot = queue.metrics().snapshot();
    assert_eq!(snapshot.drained, 1);
    assert_eq!(snapshot.pending, 0);
}

#[test]
fn materialization_request_converts_descriptor_to_rollup_encoder() {
    let request = MaterializationRequest {
        target: target(),
        partition_key: b"sensor-2".to_vec(),
        window_start_ts: 20_000_000,
        window_end_ts: 30_000_000,
        kind: MaterializationTaskKind::LateDataRecalculation,
        retry_count: 2,
    };

    let rollup = request.into_rollup();

    assert_eq!(rollup.target.target_table.table, "sensor_10s");
    assert_eq!(rollup.partition_key, b"sensor-2");
    assert_eq!(rollup.window_start_ts, 20_000_000);
}

#[test]
fn late_window_classifies_fresh_stale_and_drop_windows() {
    let target = target();

    assert_eq!(
        target.classify_late_window(30_000_000, 20_000_000, Duration::from_micros(10_000_000),),
        LateWindowClassification::Fresh
    );
    assert_eq!(
        target.classify_late_window(10_000_000, 30_000_000, Duration::from_micros(25_000_000),),
        LateWindowClassification::Stale
    );
    assert_eq!(
        target.classify_late_window(0, 40_000_000, Duration::from_micros(20_000_000),),
        LateWindowClassification::Drop
    );
}

fn timeseries_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "sensor".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ts".to_string(),
            type_name: "org.apache.cassandra.db.marshal.LongType".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "value".to_string(),
            type_name: "org.apache.cassandra.db.marshal.DoubleType".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn live_consolidation_schema() -> TableSchema {
    let mut schema = timeseries_schema();
    schema.extensions = HashMap::from([
        ("consolidation.interval".to_string(), "10s".to_string()),
        ("consolidation.functions".to_string(), "avg".to_string()),
        ("consolidation.target".to_string(), "sensor_10s".to_string()),
        ("consolidation.columns".to_string(), "value".to_string()),
        ("consolidation.ring_capacity".to_string(), "64".to_string()),
        (
            "consolidation.channel_capacity".to_string(),
            "8".to_string(),
        ),
    ]);
    schema
}

fn consolidation_schema_with_late_window(late_window: &str) -> TableSchema {
    let mut schema = live_consolidation_schema();
    schema.extensions.insert(
        "consolidation.late_window".to_string(),
        late_window.to_string(),
    );
    schema
}

fn consolidation_schema_with_ring_capacity(ring_capacity: &str) -> TableSchema {
    let mut schema = live_consolidation_schema();
    schema.extensions.insert(
        "consolidation.ring_capacity".to_string(),
        ring_capacity.to_string(),
    );
    schema
}

fn consolidation_schema_with_interval(interval: &str) -> TableSchema {
    let mut schema = live_consolidation_schema();
    schema
        .extensions
        .insert("consolidation.interval".to_string(), interval.to_string());
    schema
}

fn rollup_schema(
    table: &str,
    columns: &[&str],
    extensions: HashMap<String, String>,
) -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: table.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ts".to_string(),
            type_name: "org.apache.cassandra.db.marshal.LongType".to_string(),
        }],
        static_columns: vec![],
        regular_columns: columns
            .iter()
            .map(|name| ColumnDefinition {
                name: (*name).to_string(),
                type_name: "org.apache.cassandra.db.marshal.DoubleType".to_string(),
            })
            .collect(),
        extensions,
    }
}

fn sensor_10s_schema() -> TableSchema {
    rollup_schema("sensor_10s", &["value_avg"], HashMap::new())
}

fn sensor_10s_wasm_stddev_schema() -> TableSchema {
    rollup_schema("sensor_10s", &["value_stddev"], HashMap::new())
}

fn wasm_stddev_consolidation_schema() -> TableSchema {
    let mut schema = live_consolidation_schema();
    schema.extensions.insert(
        "consolidation.functions".to_string(),
        "wasm:ks.stddev".to_string(),
    );
    schema
}

fn sensor_10s_cascade_schema() -> TableSchema {
    rollup_schema(
        "sensor_10s",
        &["value_avg"],
        HashMap::from([
            ("consolidation.interval".to_string(), "20s".to_string()),
            ("consolidation.functions".to_string(), "avg".to_string()),
            ("consolidation.target".to_string(), "sensor_20s".to_string()),
            ("consolidation.columns".to_string(), "value_avg".to_string()),
            ("consolidation.ring_capacity".to_string(), "64".to_string()),
            (
                "consolidation.channel_capacity".to_string(),
                "8".to_string(),
            ),
        ]),
    )
}

fn sensor_20s_schema() -> TableSchema {
    rollup_schema("sensor_20s", &["value_avg_avg"], HashMap::new())
}

fn make_engine(dir: &std::path::Path) -> StorageEngine {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    };
    StorageEngine::new(config, None).unwrap()
}

fn key(bytes: &[u8]) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(bytes.to_vec()))
}

fn sensor_row(ts: i64, value: f64) -> Row {
    sensor_row_with_cell_timestamp(ts, value, ts)
}

fn sensor_row_with_cell_timestamp(ts: i64, value: f64, cell_timestamp: i64) -> Row {
    Row {
        clustering: ts.to_be_bytes().to_vec(),
        cells: vec![(
            0,
            CellValue::live(value.to_be_bytes().to_vec(), cell_timestamp),
        )],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(cell_timestamp),
    }
}

fn deleted_sensor_row(ts: i64, delete_timestamp: i64) -> Row {
    Row {
        clustering: ts.to_be_bytes().to_vec(),
        cells: vec![],
        deletion: DeletionTime::new(delete_timestamp, 0),
        primary_key_liveness: LivenessInfo::with_timestamp(delete_timestamp),
    }
}

fn row_double(row: &Row) -> f64 {
    let bytes = row.cells[0].1.value.as_ref().expect("live value cell");
    f64::from_be_bytes(bytes.as_slice().try_into().unwrap())
}

fn read_first_value(engine: &StorageEngine, table_id: &TableId, key: &DecoratedKey) -> Option<f64> {
    engine
        .read(table_id, key)
        .unwrap()
        .and_then(|partition| partition.rows.first().map(row_double))
}

struct RustStddevExecutor;

struct RustStddevInvocation {
    count: f64,
    mean: f64,
    m2: f64,
}

impl TimeSeriesWasmAggregateExecutor for RustStddevExecutor {
    fn start(
        &self,
        keyspace: &str,
        function_name: &str,
        arg_type: &str,
    ) -> Result<Box<dyn TimeSeriesWasmAggregateInvocation>, String> {
        assert_eq!(keyspace, "ks");
        assert_eq!(function_name, "stddev");
        assert_eq!(arg_type, "double");
        Ok(Box::new(RustStddevInvocation {
            count: 0.0,
            mean: 0.0,
            m2: 0.0,
        }))
    }
}

impl TimeSeriesWasmAggregateInvocation for RustStddevInvocation {
    fn update(&mut self, value: f64) -> Result<(), String> {
        self.count += 1.0;
        let delta = value - self.mean;
        self.mean += delta / self.count;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<f64, String> {
        Ok((self.m2 / self.count).sqrt())
    }
}

#[test]
fn keyed_time_series_window_cursor_streams_rows_without_returning_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let table_id = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    let other = key(b"sensor-2");
    engine.register_table(timeseries_schema()).unwrap();

    engine
        .write(&table_id, &sensor, sensor_row(0, 1.0), 0)
        .unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(5, 2.0), 5)
        .unwrap();
    engine
        .write(&table_id, &other, sensor_row(7, 99.0), 7)
        .unwrap();
    engine.flush(&table_id).unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(10, 3.0), 10)
        .unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(15, 4.0), 15)
        .unwrap();

    let mut values = Vec::new();
    let visited = engine
        .visit_time_series_window_rows(&table_id, &sensor, 5, 15, |row| {
            values.push(row_double(row));
            Ok(())
        })
        .unwrap();

    assert_eq!(visited, 2);
    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn keyed_time_series_window_cursor_merges_duplicate_rows_across_sources() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let table_id = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    engine.register_table(timeseries_schema()).unwrap();

    engine
        .write(&table_id, &sensor, sensor_row(5, 2.0), 5)
        .unwrap();
    engine.flush(&table_id).unwrap();
    engine
        .write(
            &table_id,
            &sensor,
            sensor_row_with_cell_timestamp(5, 7.0, 50),
            50,
        )
        .unwrap();

    let mut values = Vec::new();
    let visited = engine
        .visit_time_series_window_rows(&table_id, &sensor, 0, 10, |row| {
            values.push(row_double(row));
            Ok(())
        })
        .unwrap();

    assert_eq!(visited, 1);
    assert_eq!(values, vec![7.0]);
}

#[test]
fn keyed_time_series_window_cursor_visits_zero_for_missing_or_empty_windows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let table_id = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    engine.register_table(timeseries_schema()).unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(10, 3.0), 10)
        .unwrap();

    let mut called = false;
    assert_eq!(
        engine
            .visit_time_series_window_rows(&table_id, &key(b"missing"), 0, 20, |_| {
                called = true;
                Ok(())
            })
            .unwrap(),
        0
    );
    assert!(!called);

    assert_eq!(
        engine
            .visit_time_series_window_rows(&table_id, &sensor, 20, 30, |_| {
                called = true;
                Ok(())
            })
            .unwrap(),
        0
    );
    assert!(!called);

    assert_eq!(
        engine
            .visit_time_series_window_rows(&table_id, &sensor, 30, 30, |_| {
                called = true;
                Ok(())
            })
            .unwrap(),
        0
    );
    assert!(!called);
}

#[test]
fn keyed_time_series_window_cursor_merges_overlapping_sources_by_clustering_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let table_id = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    engine.register_table(timeseries_schema()).unwrap();

    engine
        .write(&table_id, &sensor, sensor_row(5, 1.0), 5)
        .unwrap();
    engine.flush(&table_id).unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(5, 9.0), 50)
        .unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(7, 3.0), 70)
        .unwrap();

    let mut values = Vec::new();
    let visited = engine
        .visit_time_series_window_rows(&table_id, &sensor, 0, 10, |row| {
            values.push(row_double(row));
            Ok(())
        })
        .unwrap();

    assert_eq!(visited, 2);
    assert_eq!(values, vec![9.0, 3.0]);
}

#[test]
fn storage_materialization_observability_reports_live_queue_lag() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let table_id = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    engine.register_table(live_consolidation_schema()).unwrap();

    for ts in 0..10 {
        engine
            .write(
                &table_id,
                &sensor,
                sensor_row(ts * 1_000_000, ts as f64),
                ts,
            )
            .unwrap();
    }
    engine
        .write(&table_id, &sensor, sensor_row(10_000_000, 10.0), 10_000_000)
        .unwrap();

    let mut queues = Vec::new();
    engine.visit_time_series_materialization_queues(&mut |snapshot| queues.push(snapshot));
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].source_table, table_id);
    assert_eq!(queues[0].target_table, TableId::new("ks", "sensor_10s"));
    assert_eq!(queues[0].window_start_ts, 0);
    assert_eq!(queues[0].window_end_ts, 10_000_000);
    assert_eq!(queues[0].task_type, "window_close");
    assert_eq!(queues[0].queue_depth, 1);
    assert!(queues[0].enqueued_at_ms > 0);
    assert!(queues[0].max_delay_ms > 0);

    let mut statuses = Vec::new();
    engine.visit_time_series_materialization_statuses(&mut |snapshot| statuses.push(snapshot));
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].pending_tasks, 1);
    assert_eq!(statuses[0].status, "pending");

    assert!(engine
        .process_one_time_series_materialization(&table_id)
        .unwrap());

    let mut drained = Vec::new();
    engine.visit_time_series_materialization_queues(&mut |snapshot| drained.push(snapshot));
    assert_eq!(drained[0].queue_depth, 0);
    assert_eq!(drained[0].task_type, "empty");
}

#[test]
fn storage_materialization_observability_reports_alerting_queue_state() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let table_id = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    engine
        .register_table(consolidation_schema_with_interval("1ms"))
        .unwrap();

    engine
        .write(&table_id, &sensor, sensor_row(0, 1.0), 0)
        .unwrap();
    engine
        .write(&table_id, &sensor, sensor_row(1_000, 2.0), 1_000)
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));

    let mut queues = Vec::new();
    engine.visit_time_series_materialization_queues(&mut |snapshot| queues.push(snapshot));
    assert_eq!(queues.len(), 1);
    assert!(queues[0].oldest_task_age_ms > queues[0].max_delay_ms);
    assert!(queues[0].alerting);

    let mut statuses = Vec::new();
    engine.visit_time_series_materialization_statuses(&mut |snapshot| statuses.push(snapshot));
    assert_eq!(statuses[0].status, "degraded");
}

#[test]
fn materialization_drain_writes_queryable_rollup_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let target_table = TableId::new("ks", "sensor_10s");
    let sensor = key(b"sensor-1");
    engine.register_table(sensor_10s_schema()).unwrap();
    engine.register_table(live_consolidation_schema()).unwrap();

    for ts in 0..=10 {
        engine
            .write(
                &source_table,
                &sensor,
                sensor_row(ts * 1_000_000, ts as f64),
                ts * 1_000_000,
            )
            .unwrap();
    }

    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        1
    );
    let value = read_first_value(&engine, &target_table, &sensor).unwrap();
    assert!((value - 4.5).abs() < f64::EPSILON);
}

#[test]
fn materialization_drain_streams_values_into_wasm_stddev_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let target_table = TableId::new("ks", "sensor_10s");
    let sensor = key(b"sensor-1");
    engine.set_time_series_wasm_aggregate_executor(Arc::new(RustStddevExecutor));
    engine
        .register_table(sensor_10s_wasm_stddev_schema())
        .unwrap();
    engine
        .register_table(wasm_stddev_consolidation_schema())
        .unwrap();

    for (ts, value) in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        .into_iter()
        .enumerate()
    {
        engine
            .write(
                &source_table,
                &sensor,
                sensor_row(ts as i64 * 1_000_000, value),
                ts as i64 * 1_000_000,
            )
            .unwrap();
    }
    engine
        .write(
            &source_table,
            &sensor,
            sensor_row(10_000_000, 11.0),
            10_000_000,
        )
        .unwrap();

    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        1
    );
    let value = read_first_value(&engine, &target_table, &sensor).unwrap();
    assert!((value - 2.0).abs() < 1e-10);
}

#[test]
fn rrd_materialization_failure_is_visible_in_status_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let sensor = key(b"sensor-1");
    engine
        .register_table(sensor_10s_wasm_stddev_schema())
        .unwrap();
    engine
        .register_table(wasm_stddev_consolidation_schema())
        .unwrap();

    engine
        .write(&source_table, &sensor, sensor_row(0, 2.0), 0)
        .unwrap();
    engine
        .write(
            &source_table,
            &sensor,
            sensor_row(10_000_000, 4.0),
            10_000_000,
        )
        .unwrap();

    let err = engine
        .process_one_time_series_materialization(&source_table)
        .expect_err("missing WASM aggregate executor must fail materialization");
    assert!(
        err.to_string().contains("requires WASM aggregate executor"),
        "unexpected materialization error: {err}"
    );

    let mut statuses = Vec::new();
    engine.visit_time_series_materialization_statuses(&mut |snapshot| statuses.push(snapshot));
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].status, "failed");
    assert_eq!(statuses[0].failed_tasks, 1);
    assert!(
        statuses[0]
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("requires WASM aggregate executor")),
        "failed RRD worker state must expose the last materialization error: {statuses:?}"
    );
}

#[test]
fn materialization_drain_recomputes_from_storage_when_ring_window_is_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let target_table = TableId::new("ks", "sensor_10s");
    let sensor = key(b"sensor-1");
    engine.register_table(sensor_10s_schema()).unwrap();
    engine
        .register_table(consolidation_schema_with_ring_capacity("4"))
        .unwrap();

    for ts in 0..=10 {
        engine
            .write(
                &source_table,
                &sensor,
                sensor_row(ts * 1_000_000, ts as f64),
                ts * 1_000_000,
            )
            .unwrap();
    }

    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        1
    );
    let value = read_first_value(&engine, &target_table, &sensor).unwrap();
    assert!(
        (value - 4.5).abs() < f64::EPSILON,
        "materialization must not use a partial in-memory ring as the full source of truth"
    );
}

#[test]
fn materialization_drain_excludes_row_deleted_source_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let target_table = TableId::new("ks", "sensor_10s");
    let sensor = key(b"sensor-1");
    engine.register_table(sensor_10s_schema()).unwrap();
    engine.register_table(live_consolidation_schema()).unwrap();

    engine
        .write(&source_table, &sensor, sensor_row(0, 10.0), 0)
        .unwrap();
    engine
        .write(
            &source_table,
            &sensor,
            deleted_sensor_row(0, 5_000_000),
            5_000_000,
        )
        .unwrap();
    engine
        .write(
            &source_table,
            &sensor,
            sensor_row(10_000_000, 20.0),
            10_000_000,
        )
        .unwrap();

    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        1
    );
    assert_eq!(
        read_first_value(&engine, &target_table, &sensor),
        None,
        "RRD materialization must not aggregate values suppressed by a row tombstone"
    );
}

#[test]
fn stale_late_materialization_task_is_dropped_without_rewriting_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let target_table = TableId::new("ks", "sensor_10s");
    let sensor = key(b"sensor-1");
    engine.register_table(sensor_10s_schema()).unwrap();
    engine
        .register_table(consolidation_schema_with_late_window("5s"))
        .unwrap();

    for ts in 0..=30 {
        engine
            .write(
                &source_table,
                &sensor,
                sensor_row(ts * 1_000_000, ts as f64),
                ts * 1_000_000,
            )
            .unwrap();
    }
    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        3
    );
    let before = read_first_value(&engine, &target_table, &sensor).unwrap();
    assert!((before - 4.5).abs() < f64::EPSILON);

    engine
        .write(&source_table, &sensor, sensor_row(0, 100.0), 40_000_000)
        .unwrap();
    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        1
    );

    let after = read_first_value(&engine, &target_table, &sensor).unwrap();
    assert!(
        (after - before).abs() < f64::EPSILON,
        "stale late data outside late_window must not rewrite existing rollups"
    );

    let mut statuses = Vec::new();
    engine.visit_time_series_materialization_statuses(&mut |snapshot| statuses.push(snapshot));
    assert_eq!(statuses[0].stale_drops_total, 1);
}

#[test]
fn materialization_drain_dispatches_rollup_writes_to_cascade_observers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(dir.path());
    let source_table = TableId::new("ks", "sensor");
    let tier1_table = TableId::new("ks", "sensor_10s");
    let tier2_table = TableId::new("ks", "sensor_20s");
    let sensor = key(b"sensor-1");
    engine.register_table(sensor_20s_schema()).unwrap();
    engine.register_table(sensor_10s_cascade_schema()).unwrap();
    engine.register_table(live_consolidation_schema()).unwrap();

    for ts in 0..=30 {
        engine
            .write(
                &source_table,
                &sensor,
                sensor_row(ts * 1_000_000, ts as f64),
                ts * 1_000_000,
            )
            .unwrap();
    }

    assert_eq!(
        engine
            .process_pending_time_series_materializations(8)
            .unwrap(),
        4
    );
    assert!(read_first_value(&engine, &tier1_table, &sensor).is_some());
    let value = read_first_value(&engine, &tier2_table, &sensor).unwrap();
    assert!((value - 9.5).abs() < f64::EPSILON);
}

#[tokio::test]
async fn background_materialization_worker_eventually_writes_rollups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(make_engine(dir.path()));
    let source_table = TableId::new("ks", "sensor");
    let target_table = TableId::new("ks", "sensor_10s");
    let sensor = key(b"sensor-1");
    engine.register_table(sensor_10s_schema()).unwrap();
    engine.register_table(live_consolidation_schema()).unwrap();
    let worker = StorageEngine::spawn_time_series_materialization_worker_with_config(
        engine.clone(),
        Duration::from_millis(5),
        8,
    );

    for ts in 0..=10 {
        engine
            .write(
                &source_table,
                &sensor,
                sensor_row(ts * 1_000_000, ts as f64),
                ts * 1_000_000,
            )
            .unwrap();
    }

    let mut value = None;
    for _ in 0..40 {
        if let Some(found) = read_first_value(&engine, &target_table, &sensor) {
            value = Some(found);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    worker.abort();

    let value = value.expect("background materialization worker wrote target row");
    assert!((value - 4.5).abs() < f64::EPSILON);
}
