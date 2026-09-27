#[test]
fn backpressure_writer_each_component_blocks_finish_and_preserves_bytes() {
    use crate::backpressure_test_support::WriteGate;
    use crate::pump::test_support::{install_sink_hook, PumpOverrides};
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(10);

    let mut partitions: Vec<_> = (0..3)
        .map(|n| make_wide_partition(format!("gated-{n}").as_bytes(), 100))
        .collect();
    partitions.sort_by_key(|p| p.key.token);
    for compression in [
        None,
        Some(Compression::Lz4),
        Some(Compression::Zstd { level: 1 }),
    ] {
        let options = WriteOptions {
            compression: compression.clone(),
            ..WriteOptions::default()
        };
        let mut baseline = SSTableWriter::new(options.clone(), test_header());
        for partition in &partitions {
            baseline.add_partition(partition).unwrap();
        }
        let expected = baseline.finish().unwrap();
        for component in [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "Digest.crc32",
            if compression.is_some() {
                "CompressionInfo.db"
            } else {
                "CRC.db"
            },
        ] {
            let dir = tempfile::tempdir().unwrap();
            let gate = Arc::new(WriteGate::new(DEADLINE));
            let injected_gate = Arc::clone(&gate);
            let _hook = install_sink_hook(
                dir.path().to_path_buf(),
                PumpOverrides {
                    segment_bytes: Some(4096),
                    queue_depth: Some(1),
                },
                Arc::new(move |open, sink| {
                    if open.path.file_name().unwrap() == component {
                        injected_gate.wrap(sink)
                    } else {
                        sink
                    }
                }),
            );
            let path = dir.path().to_path_buf();
            let options = options.clone();
            let partitions = partitions.clone();
            let (tx, rx) = crossbeam_channel::bounded(1);
            let worker = std::thread::spawn(move || {
                let result = (|| -> Result<_> {
                    let mut writer = SSTableWriter::new_file_backed(
                        options,
                        test_header(),
                        path.join("Data.db"),
                    )?;
                    for partition in &partitions {
                        writer.add_partition(partition)?;
                    }
                    writer.finish_to_directory(path)
                })();
                tx.send(result).unwrap();
            });
            gate.wait_for_attempts(1);
            assert_eq!(gate.progress().completed, 0, "{component}");
            assert!(
                rx.try_recv().is_err(),
                "writer finished while {component} was blocked"
            );
            gate.open();
            let actual = rx
                .recv_timeout(DEADLINE)
                .unwrap()
                .unwrap()
                .read_to_memory()
                .unwrap();
            worker.join().unwrap();
            assert!(gate.progress().completed > 0);
            assert_eq!(actual.data, expected.data, "{component}");
            assert_eq!(actual.partitions, expected.partitions, "{component}");
            assert_eq!(actual.rows, expected.rows, "{component}");
            assert_eq!(actual.filter, expected.filter, "{component}");
            assert_eq!(actual.statistics, expected.statistics, "{component}");
            assert_eq!(actual.crc, expected.crc, "{component}");
            assert_eq!(actual.digest, expected.digest, "{component}");
            assert_eq!(
                actual.compression_info, expected.compression_info,
                "{component}"
            );
            assert_eq!(actual.toc, expected.toc, "{component}");
        }
    }
}

/// B3/B4: stall the first full compression batch independently from the real
/// Data.db sink, then stall that sink after the codec is released.
#[test]
fn backpressure_codec_and_data_sink_are_independent_bounded_stages() {
    use crate::backpressure_test_support::WriteGate;
    use crate::pump::test_support::{install_sink_hook, PumpOverrides};
    use std::time::Duration;
    const DEADLINE: Duration = Duration::from_secs(10);
    let dir = tempfile::tempdir().unwrap();
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
    let options = WriteOptions {
        compression: Some(Compression::Lz4),
        chunk_size: 4096,
        ..WriteOptions::default()
    };
    let mut partition = make_wide_partition(b"codec-gate", 1);
    let mut seed = 1234567u32;
    partition.rows[0].cells[0].1.value = Some(
        (0..1024 * 1024)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                seed as u8
            })
            .collect(),
    );
    let mut baseline = SSTableWriter::new(options.clone(), test_header());
    baseline.add_partition(&partition).unwrap();
    let expected = baseline.finish().unwrap();
    let mut writer =
        SSTableWriter::new_file_backed(options, test_header(), dir.path().join("Data.db")).unwrap();
    let DataSink::Stream(stream) = &mut writer.data_buf else {
        panic!("file-backed writer must stream")
    };
    let compressor = stream.compressor.as_mut().unwrap();
    let capacities = (
        compressor.inputs.len(),
        compressor.outputs.len(),
        compressor.lens.capacity(),
        compressor.written_lens.capacity(),
    );
    let (arrived_tx, arrived_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    compressor.before_first_batch = Some(Box::new(move || {
        arrived_tx.send(()).unwrap();
        release_rx
            .recv_timeout(DEADLINE)
            .expect("test must release codec");
    }));
    let path = dir.path().to_path_buf();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        writer.add_partition(&partition).unwrap();
        let DataSink::Stream(stream) = &writer.data_buf else {
            unreachable!()
        };
        let compressor = stream.compressor.as_ref().unwrap();
        assert_eq!(
            (
                compressor.inputs.len(),
                compressor.outputs.len(),
                compressor.lens.capacity(),
                compressor.written_lens.capacity()
            ),
            capacities
        );
        done_tx.send(writer.finish_to_directory(path)).unwrap();
    });
    arrived_rx.recv_timeout(DEADLINE).unwrap();
    assert_eq!(
        gate.progress().attempted,
        0,
        "codec stall must not issue Data.db writes"
    );
    assert!(done_rx.try_recv().is_err());
    release_tx.send(()).unwrap();
    gate.wait_for_attempts(1);
    assert_eq!(gate.progress().completed, 0);
    assert!(
        done_rx.try_recv().is_err(),
        "released codec must still honor downstream backpressure"
    );
    gate.open();
    let actual = done_rx
        .recv_timeout(DEADLINE)
        .unwrap()
        .unwrap()
        .read_to_memory()
        .unwrap();
    worker.join().unwrap();
    assert_eq!(actual.data, expected.data);
    assert_eq!(actual.compression_info, expected.compression_info);
    assert_eq!(actual.digest, expected.digest);
}
