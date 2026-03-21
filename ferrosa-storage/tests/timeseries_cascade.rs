//! Integration test: time-series consolidation cascade.
//!
//! Tests the full path from write -> ring buffer -> boundary detection ->
//! consolidation task -> value consolidation, verifying that the aggregator,
//! ring buffer, and consolidation functions work together correctly.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::observer::WriteObserver;
use ferrosa_storage::timeseries::aggregator::{
    ConsolidationMetrics, ConsolidationTask, ConsolidationWorker, TimeSeriesAggregator,
};
use ferrosa_storage::timeseries::config::ConsolidationConfig;
use ferrosa_storage::timeseries::consolidation::{consolidate_values, ConsolidationFn};
use ferrosa_storage::TableId;

fn make_mutation(table: &str, pk: &[u8], ts: i64, value: f64) -> ferrosa_storage::Mutation {
    ferrosa_storage::Mutation {
        keyspace: "ks".to_string(),
        table: table.to_string(),
        key: DecoratedKey::new(PartitionKey::new(pk.to_vec())),
        rows: vec![Row {
            clustering: ts.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(value.to_be_bytes().to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }],
        timestamp: ts,
    }
}

#[test]
fn single_tier_consolidation_produces_task() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000), // 10s in micros
        functions: vec![
            ConsolidationFn::Min,
            ConsolidationFn::Max,
            ConsolidationFn::Avg,
        ],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let table_id = TableId::new("ks", "sensor");
    let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

    // Write 10 points in the [0, 10M) window.
    for i in 0..10 {
        let ts = i as i64 * 1_000_000;
        aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", ts, i as f64));
    }

    // Cross boundary.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 10_000_000, 10.0));

    let task = rx.try_recv().expect("expected consolidation task");
    match task {
        ConsolidationTask::BoundaryCrossed {
            window_entries,
            window_start_ts,
            ..
        } => {
            assert_eq!(window_start_ts, 0);
            // Verify window entries contain the [0, 10M) data.
            assert!(!window_entries.is_empty());

            // Consolidate the window.
            let values: Vec<f64> = window_entries.iter().map(|e| e.values[0]).collect();
            let funcs = vec![
                ConsolidationFn::Min,
                ConsolidationFn::Max,
                ConsolidationFn::Avg,
            ];
            let results = consolidate_values(&values, &funcs);

            assert!((results[0] - 0.0).abs() < f64::EPSILON); // min
            assert!((results[1] - 9.0).abs() < f64::EPSILON); // max
            assert!((results[2] - 4.5).abs() < f64::EPSILON); // avg
        }
        _ => panic!("expected BoundaryCrossed"),
    }
}

#[test]
fn multi_window_cascade_produces_sequential_tasks() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000), // 10s
        functions: vec![ConsolidationFn::Avg],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let table_id = TableId::new("ks", "sensor");
    let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

    // Window 1: [0, 10M)
    for i in 0..10 {
        aggregator.on_write(
            &table_id,
            &make_mutation("sensor", b"s1", i * 1_000_000, i as f64),
        );
    }

    // Cross first boundary.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 10_000_000, 10.0));

    let task1 = rx.try_recv().expect("expected first task");
    assert!(matches!(
        task1,
        ConsolidationTask::BoundaryCrossed {
            window_start_ts: 0,
            ..
        }
    ));

    // Window 2: [10M, 20M)
    for i in 11..20 {
        aggregator.on_write(
            &table_id,
            &make_mutation("sensor", b"s1", i * 1_000_000, i as f64),
        );
    }

    // Cross second boundary.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 20_000_000, 20.0));

    let task2 = rx.try_recv().expect("expected second task");
    assert!(matches!(
        task2,
        ConsolidationTask::BoundaryCrossed {
            window_start_ts: 10_000_000,
            ..
        }
    ));
}

#[test]
fn worker_consolidates_window_end_to_end() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000),
        functions: vec![
            ConsolidationFn::Min,
            ConsolidationFn::Max,
            ConsolidationFn::Avg,
            ConsolidationFn::StdDev,
        ],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let metrics = Arc::new(ConsolidationMetrics::new());
    let table_id = TableId::new("ks", "sensor");
    let aggregator = TimeSeriesAggregator::new(config.clone(), table_id.clone(), vec![0], tx);
    let worker = ConsolidationWorker::new(config, rx, metrics.clone());

    // Write points: values 0-9.
    for i in 0..10 {
        aggregator.on_write(
            &table_id,
            &make_mutation("sensor", b"s1", i * 1_000_000, i as f64),
        );
    }

    // Cross boundary.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 10_000_000, 10.0));

    // Get the task and process it through the worker.
    let task = worker.task_rx().try_recv().expect("expected task");
    if let ConsolidationTask::BoundaryCrossed { window_entries, .. } = task {
        let results = worker.consolidate_window(&window_entries, 0);
        assert_eq!(results.len(), 4); // min, max, avg, stddev
        assert!((results[0] - 0.0).abs() < f64::EPSILON); // min
        assert!((results[1] - 9.0).abs() < f64::EPSILON); // max
        assert!((results[2] - 4.5).abs() < f64::EPSILON); // avg
        assert!(results[3] > 0.0); // stddev > 0
    } else {
        panic!("expected BoundaryCrossed");
    }
}

#[test]
fn multiple_partitions_produce_independent_tasks() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000),
        functions: vec![ConsolidationFn::Avg],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let table_id = TableId::new("ks", "sensor");
    let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

    // Write to two different partition keys.
    for i in 0..10 {
        let ts = i * 1_000_000;
        aggregator.on_write(&table_id, &make_mutation("sensor", b"pk1", ts, i as f64));
        aggregator.on_write(
            &table_id,
            &make_mutation("sensor", b"pk2", ts, (i * 10) as f64),
        );
    }

    // Cross boundary for both partitions.
    aggregator.on_write(
        &table_id,
        &make_mutation("sensor", b"pk1", 10_000_000, 10.0),
    );
    aggregator.on_write(
        &table_id,
        &make_mutation("sensor", b"pk2", 10_000_000, 100.0),
    );

    // Should get two tasks (one per partition).
    let task1 = rx.try_recv().expect("expected task for pk1");
    let task2 = rx.try_recv().expect("expected task for pk2");

    assert!(matches!(task1, ConsolidationTask::BoundaryCrossed { .. }));
    assert!(matches!(task2, ConsolidationTask::BoundaryCrossed { .. }));

    // Verify different partition keys.
    if let (
        ConsolidationTask::BoundaryCrossed {
            partition_key: pk1, ..
        },
        ConsolidationTask::BoundaryCrossed {
            partition_key: pk2, ..
        },
    ) = (&task1, &task2)
    {
        assert_ne!(pk1, pk2);
    }
}

// --- Task 24: Late data cascade ---

#[test]
fn late_data_triggers_reagg_task() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000), // 10s
        functions: vec![ConsolidationFn::Avg],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let table_id = TableId::new("ks", "sensor");
    let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

    // Establish a boundary at 20M by writing at 15M then 25M.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 15_000_000, 1.0));
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 25_000_000, 2.0));
    let _ = rx.try_recv(); // consume BoundaryCrossed

    // Now send late data at ts=5M.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 5_000_000, 99.0));

    let task = rx.try_recv().expect("expected LateData task");
    match task {
        ConsolidationTask::LateData {
            late_timestamp,
            window_start_ts,
            ..
        } => {
            assert_eq!(late_timestamp, 5_000_000);
            assert_eq!(window_start_ts, 0); // floor(5M / 10M) * 10M = 0
        }
        _ => panic!("expected LateData"),
    }
}

#[test]
fn late_data_multiple_points_same_window() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000),
        functions: vec![ConsolidationFn::Avg],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let table_id = TableId::new("ks", "sensor");
    let aggregator = TimeSeriesAggregator::new(config, table_id.clone(), vec![0], tx);

    // Establish boundary.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 15_000_000, 1.0));
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 25_000_000, 2.0));
    let _ = rx.try_recv(); // consume BoundaryCrossed

    // Multiple late points in the same window [0, 10M).
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 2_000_000, 10.0));
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 4_000_000, 20.0));
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 6_000_000, 30.0));

    // Each late point generates a LateData task (debouncing is at the worker level).
    let mut late_count = 0;
    while rx.try_recv().is_ok() {
        late_count += 1;
    }
    assert!(
        late_count >= 3,
        "expected at least 3 LateData tasks, got {late_count}"
    );
}

#[test]
fn metrics_track_late_arrivals() {
    let config = ConsolidationConfig {
        interval: Duration::from_micros(10_000_000),
        functions: vec![ConsolidationFn::Avg],
        target_table: "sensor_10s".to_string(),
        columns: vec!["value".into()],
        ring_capacity: 64,
        max_rings: 100,
        ..ConsolidationConfig::default()
    };

    let metrics = Arc::new(ConsolidationMetrics::new());
    let (tx, rx) = mpsc::sync_channel::<ConsolidationTask>(100);
    let table_id = TableId::new("ks", "sensor");
    let aggregator =
        TimeSeriesAggregator::with_metrics(config, table_id.clone(), vec![0], tx, metrics.clone());

    // Establish boundary and cross it.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 15_000_000, 1.0));
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 25_000_000, 2.0));

    // Drain tasks.
    while rx.try_recv().is_ok() {}

    // Send late data.
    aggregator.on_write(&table_id, &make_mutation("sensor", b"s1", 3_000_000, 99.0));

    let snap = metrics.snapshot();
    assert!(
        snap.windows_consolidated >= 1,
        "expected at least 1 window consolidated"
    );
    assert!(snap.late_arrivals >= 1, "expected at least 1 late arrival");
}
