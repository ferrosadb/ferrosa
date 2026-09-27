// Acceptance through the real engine and real file sinks; hooks only record.
#[tokio::test]
async fn wiring_flush_compaction_restart_components() {
    use ferrosa_sstable::pump::test_support::{PumpOverrides, PumpTrace};
    let dir = tempfile::tempdir().unwrap();
    let trace = PumpTrace::install(
        dir.path().to_path_buf(),
        PumpOverrides {
            segment_bytes: Some(262_144),
            queue_depth: Some(1),
        },
    );
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.flush_threshold_bytes = u64::MAX;
    config.commit_log.segment_size = 1024 * 1024;
    let engine = StorageEngine::new(config, None).unwrap();
    let mut schemas = Vec::new();
    for compressed in [false, true] {
        let mut schema = test_schema();
        schema.table = if compressed { "compressed" } else { "plain" }.to_owned();
        schema.extensions.insert(
            "compression.class".to_owned(),
            if compressed { "lz4" } else { "none" }.to_owned(),
        );
        engine.register_table(schema.clone()).unwrap();
        let tid = TableId::new(&schema.keyspace, &schema.table);
        for batch in 0..2 {
            for i in 0..4 {
                let n = batch * 4 + i;
                let value = vec![b'a' + n as u8; if n == 0 { 300_000 } else { 64 }];
                engine
                    .write(
                        &tid,
                        &make_key(&format!("k{n}")),
                        make_row(&value, 1000 + n),
                        1000 + n,
                    )
                    .unwrap();
            }
            engine.flush(&tid).unwrap();
        }
        assert_eq!(engine.sstable_count(&tid), 2);
        let checksum = if compressed {
            "CompressionInfo.db"
        } else {
            "CRC.db"
        };
        assert_eq!(
            trace
                .files()
                .iter()
                .filter(
                    |f| f.opened.path.starts_with(engine.table_sstable_dir(&tid))
                        && f.opened.path.ends_with(checksum)
                )
                .count(),
            2
        );

        schemas.push(schema);
    }
    trace.assert_complete_sstables(4);
    engine.force_compact_all();
    assert!(engine.await_compaction_result(std::time::Duration::from_secs(60)));
    engine.poll_compactions().await;
    // Two independent table tasks may finish on different worker turns.
    if schemas
        .iter()
        .any(|s| engine.sstable_count(&TableId::new(&s.keyspace, &s.table)) != 1)
    {
        assert!(engine.await_compaction_result(std::time::Duration::from_secs(60)));
        engine.poll_compactions().await;
    }
    for schema in &schemas {
        assert_eq!(
            engine.sstable_count(&TableId::new(&schema.keyspace, &schema.table)),
            1
        );
    }
    trace.assert_complete_sstables(6);
    for file in trace
        .files()
        .into_iter()
        .filter(|f| f.opened.path.ends_with("Data.db"))
    {
        assert_eq!(file.opened.segment, 262_144);
        assert_eq!(file.opened.depth, 1);
    }
    drop(engine);
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    for schema in schemas {
        let tid = TableId::new(&schema.keyspace, &schema.table);
        engine.register_table(schema).unwrap();
        for n in 0..8 {
            let p = engine
                .read(&tid, &make_key(&format!("k{n}")))
                .unwrap()
                .expect("persisted partition");
            assert_eq!(p.rows.len(), 1);
            assert_eq!(
                p.rows[0].cells[0].1.value.as_deref(),
                Some(vec![b'a' + n as u8; if n == 0 { 300_000 } else { 64 }].as_slice())
            );
        }
    }
}

#[test]
fn wiring_runtime_settings_preserve_component_bytes() {
    use ferrosa_sstable::pump::test_support::{PumpOverrides, PumpTrace};
    if let Some(root) = std::env::var_os("FERROSA_TEST_WIRING_CHILD_ROOT") {
        let root = std::path::PathBuf::from(root);
        let trace = PumpTrace::install(root.clone(), PumpOverrides::default());
        let mut config = StorageEngineConfig::test_config(&root);
        config.commit_log.segment_size = 1024 * 1024;
        config.flush_threshold_bytes = u64::MAX;
        let engine = StorageEngine::new(config, None).unwrap();
        for compressed in [false, true] {
            let mut schema = test_schema();
            schema.table = if compressed { "compressed" } else { "plain" }.to_owned();
            schema.extensions.insert(
                "compression.class".to_owned(),
                if compressed { "lz4" } else { "none" }.to_owned(),
            );
            let tid = TableId::new(&schema.keyspace, &schema.table);
            engine.register_table(schema).unwrap();
            engine
                .write(
                    &tid,
                    &make_key("wide"),
                    make_row(&vec![b'x'; 300_000], 1000),
                    1000,
                )
                .unwrap();
            engine.flush(&tid).unwrap();
            let dir = engine.table_sstable_dir(&tid);
            let target = crate::flush::FileFlushTarget::new_starting_at(dir.clone()).unwrap();
            let generations = StorageEngine::list_generations_in_dir(&dir);
            assert_eq!(generations.len(), 1);
            let reader =
                crate::flush::FlushTarget::open_reader(&target, &dir, generations[0]).unwrap();
            assert!(reader.digest_loaded());
            reader.verify_digest().unwrap();
        }
        trace.assert_complete_sstables(2);
        let config = ferrosa_sstable::pump::PumpConfig::from_env();
        for file in trace.files() {
            assert_eq!(
                file.opened.mode,
                ferrosa_sstable::direct::DirectMode::Buffered
            );
            assert_eq!(file.opened.segment, config.segment_bytes);
            if file.opened.path.ends_with("Data.db") {
                assert_eq!(file.opened.depth, config.queue_depth);
            }
        }
    } else {
        // Environment policy is isolated in child processes: other tests can
        // keep running, and no process-global setter races their file opens.
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        for (dir, segment, depth) in [(a.path(), "262144", "1"), (b.path(), "524288", "3")] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::tests::wiring_runtime_settings_preserve_component_bytes",
                    "--nocapture",
                ])
                .env("FERROSA_TEST_WIRING_CHILD_ROOT", dir)
                .env("FERROSA_SSTABLE_DIRECT_IO", "0")
                .env("FERROSA_SSTABLE_WRITE_SEGMENT_BYTES", segment)
                .env("FERROSA_SSTABLE_WRITE_QUEUE_DEPTH", depth)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "settings child failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        fn components(
            root: &std::path::Path,
        ) -> std::collections::BTreeMap<(String, String), Vec<u8>> {
            let mut pending = vec![root.to_path_buf()];
            let mut files = std::collections::BTreeMap::new();
            while let Some(dir) = pending.pop() {
                for entry in std::fs::read_dir(&dir).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        pending.push(path);
                    } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if let Some(component) =
                            ferrosa_sstable::pump::component_metrics::COMPONENTS
                                .iter()
                                .find(|component| name.ends_with(**component))
                        {
                            let table = path
                                .parent()
                                .unwrap()
                                .file_name()
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned();
                            assert!(files
                                .insert(
                                    (table, (*component).to_owned()),
                                    std::fs::read(&path).unwrap()
                                )
                                .is_none());
                        }
                    }
                }
            }
            files
        }
        let baseline = components(a.path());
        assert_eq!(baseline.len(), 16);
        assert_eq!(
            baseline,
            components(b.path()),
            "runtime pump settings must not change persisted component bytes"
        );
    }
}
