// Regression tests for the 2026-10-05 diabolical finding: `finalize_compactions`
// ran its blocking filesystem steps (fsync, rename, unlink, recursive rmdir,
// digest read) inline on the async runtime. On a CPU-scarce host that parked a
// worker long enough to delay the CQL runtime's liveness task — a 540 ms
// `ferrosa_sched_runtime_stall_max_micros` event, seen by clients as six
// `Query timed out after PT2S` reads. These tests pin the fix: those steps now
// run on the blocking pool, and the crash-injection seam still fires and is
// still scoped.

#[test]
fn finalize_digest_read_runs_on_the_blocking_pool() {
    let dir = tempfile::tempdir().unwrap();
    let gen = "42";
    // `Digest.crc32` is the decimal ASCII CRC32 of Data.db, not raw bytes.
    std::fs::write(
        dir.path().join(format!("{gen}-Digest.crc32")),
        format!("{}", 0xDEAD_BEEFu32),
    )
    .unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let d = dir.path().to_path_buf();
    let digest =
        rt.block_on(async move { StorageEngine::offload_blocking(move || {
            StorageEngine::read_generation_digest(&d, gen)
        })
        .await });
    assert_eq!(digest.unwrap().unwrap(), 0xDEAD_BEEF);
}

#[tokio::test]
async fn finalize_compaction_offloads_blocking_steps_and_still_commits() {
    let before = FINALIZE_OFFLOADED_STEPS.load(std::sync::atomic::Ordering::Relaxed);
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.flush_threshold_bytes = u64::MAX;
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();
    let tid = table_id();
    for i in 0..4 {
        let value = vec![b'a' + i as u8; 64];
        engine
            .write(&tid, &make_key(&format!("k{i}")), make_row(&value, 1000 + i), 1000 + i)
            .unwrap();
    }
    engine.flush(&tid).unwrap();
    for i in 0..4 {
        let value = vec![b'm' + i as u8; 64];
        engine
            .write(&tid, &make_key(&format!("m{i}")), make_row(&value, 2000 + i), 2000 + i)
            .unwrap();
    }
    engine.flush(&tid).unwrap();
    assert_eq!(engine.sstable_count(&tid), 2);

    engine.force_compact_all();
    // `drive_compactions_until_idle` polls until the executed result is
    // finalized through the offloaded path.
    engine
        .drive_compactions_until_idle(&tid, std::time::Duration::from_secs(60))
        .await;

    assert_eq!(engine.sstable_count(&tid), 1, "compaction output must be swapped in");
    let after = FINALIZE_OFFLOADED_STEPS.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "finalize must push its blocking I/O onto the blocking pool (was {before}, now {after})"
    );
}
