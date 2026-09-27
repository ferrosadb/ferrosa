/// E1: actual file-backed flush stalls, active memtable admission plateaus,
/// rejected writes do not grow it, and accepted rows survive release/flush.
#[test]
fn backpressure_e1_gated_flush_bounds_active_memtable_and_recovers() {
    use ferrosa_sstable::backpressure_test_support::WriteGate;
    use ferrosa_sstable::pump::test_support::{install_sink_hook, PumpOverrides};
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(20);
    const THRESHOLD: u64 = 64 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.memtable_backpressure_bytes = THRESHOLD;
    config.compaction.min_threshold = 100;
    config.commit_log.segment_size = 1024 * 1024;
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine.register_table(test_schema()).unwrap();
    let tid = table_id();
    let value = vec![57; 16 * 1024];
    engine
        .write(
            &tid,
            &make_key("before-flush"),
            make_row(&value, 1000),
            1000,
        )
        .unwrap();
    let gate = Arc::new(WriteGate::new(DEADLINE));
    let injected = Arc::clone(&gate);
    let _hook = install_sink_hook(
        dir.path().to_path_buf(),
        PumpOverrides {
            segment_bytes: Some(4096),
            queue_depth: Some(1),
        },
        Arc::new(move |open, sink| {
            if open.path.file_name().unwrap() == "Data.db" {
                injected.wrap(sink)
            } else {
                sink
            }
        }),
    );
    let flushing = Arc::clone(&engine);
    let flush_tid = tid.clone();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        tx.send(flushing.flush(&flush_tid)).unwrap();
    });
    gate.wait_for_attempts(1);
    assert_eq!(gate.progress().completed, 0);
    assert_eq!(
        engine.sstable_count(&tid),
        0,
        "blocked output must not be published"
    );
    let mut accepted = vec!["before-flush".to_owned()];
    let mut rejected = false;
    for n in 0..16 {
        let key = format!("during-flush-{n}");
        match engine.write(&tid, &make_key(&key), make_row(&value, 1000), 1000) {
            Ok(()) => accepted.push(key),
            Err(error) => {
                assert!(error.to_string().contains("overloaded"), "{error}");
                rejected = true;
                break;
            }
        }
    }
    assert!(
        rejected,
        "bounded writer loop must reach admission rejection"
    );
    let plateau = engine.memtable_size(&tid);
    assert!(plateau >= THRESHOLD as usize);
    assert!(
        plateau < THRESHOLD as usize + 2 * value.len(),
        "at most one accepted row crosses the threshold"
    );
    for _ in 0..100 {
        let error = engine
            .write(&tid, &make_key("rejected"), make_row(&value, 1000), 1000)
            .unwrap_err();
        assert!(error.to_string().contains("overloaded"), "{error}");
        assert_eq!(engine.memtable_size(&tid), plateau);
    }
    assert!(
        rx.try_recv().is_err(),
        "flush returned before device release"
    );
    gate.open();
    rx.recv_timeout(DEADLINE).unwrap().unwrap();
    worker.join().unwrap();
    engine.flush(&tid).unwrap();
    assert_eq!(engine.memtable_size(&tid), 0);
    engine
        .write(
            &tid,
            &make_key("after-release"),
            make_row(&value, 1000),
            1000,
        )
        .unwrap();
    accepted.push("after-release".to_owned());
    engine.flush(&tid).unwrap();
    for key in accepted {
        let partition = engine.read(&tid, &make_key(&key)).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells[0].1.value.as_ref().unwrap(), &value);
    }
    assert!(engine.read(&tid, &make_key("rejected")).unwrap().is_none());
    engine.shutdown().unwrap();
}

fn backpressure_engine_with_inputs(
    dir: &std::path::Path,
    name: &str,
) -> (Arc<StorageEngine>, TableId, Vec<u8>) {
    let mut config = StorageEngineConfig::test_config(dir);
    config.compaction.min_threshold = 100;
    config.commit_log.segment_size = 1024 * 1024;
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    let mut schema = test_schema();
    schema.table = name.to_owned();
    schema.regular_columns[0].type_name = "org.apache.cassandra.db.marshal.BytesType".to_owned();
    let tid = TableId::new(&schema.keyspace, &schema.table);
    engine.register_table(schema).unwrap();
    // Deterministic incompressible data ensures output exceeds the two-segment ring.
    let mut seed = 0x1729_6d5au32;
    let value: Vec<u8> = (0..64 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    for key in ["input-one", "input-two"] {
        engine
            .write(&tid, &make_key(key), make_row(&value, 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
    }
    assert_eq!(engine.sstable_count(&tid), 2);
    (engine, tid, value)
}

/// E2/E3: actual compaction output is stalled while a separate real flush
/// completes; input retirement waits for the compaction to resume and verify.
#[test]
fn backpressure_e3_flush_completes_while_compaction_remains_gated() {
    use ferrosa_sstable::backpressure_test_support::WriteGate;
    use ferrosa_sstable::pump::test_support::{install_sink_hook, PumpOverrides};
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(20);
    let dir = tempfile::tempdir().unwrap();
    let (engine, tid, value) = backpressure_engine_with_inputs(dir.path(), "backpressure_e3");
    let compact_gate = Arc::new(WriteGate::new(DEADLINE));
    let flush_gate = Arc::new(WriteGate::new(DEADLINE));
    let compact = Arc::clone(&compact_gate);
    let flush = Arc::clone(&flush_gate);
    let compact_root = dir.path().join("compaction");
    let _hook = install_sink_hook(
        dir.path().to_path_buf(),
        PumpOverrides {
            segment_bytes: Some(4096),
            queue_depth: Some(1),
        },
        Arc::new(move |open, sink| {
            if open.path.file_name().unwrap() != "Data.db" {
                return sink;
            }
            if open.path.starts_with(&compact_root) {
                compact.wrap(sink)
            } else {
                flush.wrap(sink)
            }
        }),
    );
    assert!(matches!(
        engine
            .schedule_incremental_compaction(&tid, 2, 1024 * 1024)
            .unwrap(),
        IncrementalCompactionSchedule::Scheduled { .. }
    ));
    compact_gate.wait_for_attempts(1);
    assert_eq!(
        engine.sstable_count(&tid),
        2,
        "inputs retired before output verification"
    );
    engine
        .write(
            &tid,
            &make_key("independent-flush"),
            make_row(&value, 1000),
            1000,
        )
        .unwrap();
    let flushing = Arc::clone(&engine);
    let flush_tid = tid.clone();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        tx.send(flushing.flush(&flush_tid)).unwrap();
    });
    flush_gate.wait_for_attempts(1);
    assert_eq!(compact_gate.progress().completed, 0);
    assert_eq!(flush_gate.progress().completed, 0);
    flush_gate.open();
    rx.recv_timeout(DEADLINE).unwrap().unwrap();
    worker.join().unwrap();
    assert_eq!(
        compact_gate.progress().completed,
        0,
        "flush must finish independently of compaction permits"
    );
    assert_eq!(engine.sstable_count(&tid), 3);
    assert_eq!(engine.compaction_executor.pending_result_count(), 0);
    compact_gate.open();
    assert!(engine.compaction_executor.await_result_available(DEADLINE));
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(engine.poll_compactions());
    assert_eq!(engine.sstable_count(&tid), 2);
    for key in ["input-one", "input-two", "independent-flush"] {
        let partition = engine.read(&tid, &make_key(key)).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells[0].1.value.as_ref().unwrap(), &value);
    }
    engine.shutdown().unwrap();
}

/// E4: cancellation wakes the producer; shutdown then joins after the test
/// releases the device. An arbitrary in-kernel pwrite cannot be interrupted.
#[test]
fn backpressure_e4_shutdown_cancels_then_joins_after_device_release() {
    use ferrosa_sstable::backpressure_test_support::WriteGate;
    use ferrosa_sstable::pump::test_support::{install_sink_hook, PumpOverrides};
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(20);
    let dir = tempfile::tempdir().unwrap();
    let (engine, tid, value) = backpressure_engine_with_inputs(dir.path(), "backpressure_e4");
    let gate = Arc::new(WriteGate::new(DEADLINE));
    let injected = Arc::clone(&gate);
    let _hook = install_sink_hook(
        dir.path().join("compaction"),
        PumpOverrides {
            segment_bytes: Some(4096),
            queue_depth: Some(1),
        },
        Arc::new(move |open, sink| {
            if open.path.file_name().unwrap() == "Data.db" {
                injected.wrap(sink)
            } else {
                sink
            }
        }),
    );
    assert!(matches!(
        engine
            .schedule_incremental_compaction(&tid, 2, 1024 * 1024)
            .unwrap(),
        IncrementalCompactionSchedule::Scheduled { .. }
    ));
    gate.wait_for_attempts(1);
    let cancel = crate::compaction::cancel_harness::cancel_token_for_scope(&tid.to_string())
        .expect("live compaction token");
    let cancelled = cancel.closed();
    let shutting_down = Arc::clone(&engine);
    let (tx, rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        tx.send(shutting_down.shutdown()).unwrap();
    });
    assert_eq!(
        cancelled.recv_timeout(DEADLINE),
        Err(crossbeam_channel::RecvTimeoutError::Disconnected)
    );
    assert!(cancel.is_cancelled());
    assert_eq!(gate.progress().completed, 0);
    assert!(
        rx.try_recv().is_err(),
        "shutdown must join the outstanding device call"
    );
    gate.open();
    rx.recv_timeout(DEADLINE).unwrap().unwrap();
    worker.join().unwrap();
    assert_eq!(
        engine.sstable_count(&tid),
        2,
        "cancelled output must not replace live inputs"
    );
    assert_eq!(
        StorageEngine::list_generations_in_dir(&dir.path().join("sstables").join(tid.to_string()))
            .len(),
        2
    );
    let staging = dir
        .path()
        .join("compaction")
        .join(tid.to_string())
        .join(".sstable-staging");
    if staging.exists() {
        assert_eq!(
            std::fs::read_dir(staging).unwrap().count(),
            0,
            "cancelled staging must be cleaned up"
        );
    }
    let results = engine.compaction_executor.poll_results();
    assert!(results.iter().all(|result| result.cancel.is_cancelled()));
    for key in ["input-one", "input-two"] {
        let partition = engine.read(&tid, &make_key(key)).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells[0].1.value.as_ref().unwrap(), &value);
    }
}

#[test]
fn backpressure_b8_flush_readback_holds_publication_and_wal_retirement() {
    use ferrosa_sstable::backpressure_test_support::WriteGate;
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(20);
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.commit_log.segment_size = 512;
    config.compaction.min_threshold = 100;
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine.register_table(test_schema()).unwrap();
    let tid = table_id();
    for n in 0..20 {
        engine
            .write(
                &tid,
                &make_key(&format!("wal-{n}")),
                make_row(b"value", 1000 + n),
                1000 + n,
            )
            .unwrap();
    }
    let closed_before = engine.commit_log.closed_segment_count();
    assert!(closed_before >= 2);
    let gate = Arc::new(WriteGate::new(DEADLINE));
    let injected = Arc::clone(&gate);
    let _hook = crate::flush::readback_test_support::install(
        dir.path().to_path_buf(),
        Arc::new(move || injected.checkpoint()),
    );
    let flushing = Arc::clone(&engine);
    let flush_tid = tid.clone();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        tx.send(flushing.flush(&flush_tid)).unwrap();
    });
    gate.wait_for_attempts(1);
    assert_eq!(engine.sstable_count(&tid), 0);
    assert_eq!(engine.commit_log.closed_segment_count(), closed_before);
    assert!(rx.try_recv().is_err());
    gate.open();
    rx.recv_timeout(DEADLINE).unwrap().unwrap();
    worker.join().unwrap();
    assert_eq!(engine.sstable_count(&tid), 1);
    assert!(engine.commit_log.closed_segment_count() < closed_before);
    for n in 0..20 {
        assert!(engine
            .read(&tid, &make_key(&format!("wal-{n}")))
            .unwrap()
            .is_some());
    }
    engine.shutdown().unwrap();
}

#[test]
fn backpressure_b8_compaction_readback_holds_input_retirement() {
    use ferrosa_sstable::backpressure_test_support::WriteGate;
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(20);
    let dir = tempfile::tempdir().unwrap();
    let (engine, tid, value) = backpressure_engine_with_inputs(dir.path(), "backpressure_b8");
    let gate = Arc::new(WriteGate::new(DEADLINE));
    let injected = Arc::clone(&gate);
    let _hook = crate::flush::readback_test_support::install(
        dir.path().join("compaction"),
        Arc::new(move || injected.checkpoint()),
    );
    assert!(matches!(
        engine
            .schedule_incremental_compaction(&tid, 2, 1024 * 1024)
            .unwrap(),
        IncrementalCompactionSchedule::Scheduled { .. }
    ));
    gate.wait_for_attempts(1);
    assert_eq!(engine.sstable_count(&tid), 2);
    assert_eq!(engine.compaction_executor.pending_result_count(), 0);
    gate.open();
    assert!(engine.compaction_executor.await_result_available(DEADLINE));
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(engine.poll_compactions());
    assert_eq!(engine.sstable_count(&tid), 1);
    for key in ["input-one", "input-two"] {
        let partition = engine.read(&tid, &make_key(key)).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells[0].1.value.as_ref().unwrap(), &value);
    }
    engine.shutdown().unwrap();
}
