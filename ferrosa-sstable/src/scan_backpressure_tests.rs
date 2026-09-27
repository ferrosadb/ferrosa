type BufferAddresses = Arc<Mutex<std::collections::BTreeSet<usize>>>;

struct BufferProbe {
    data: Arc<Vec<u8>>,
    addresses: BufferAddresses,
    fail_once_at: Option<u64>,
    failed: std::sync::atomic::AtomicBool,
}
impl ReadAt for BufferProbe {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        self.addresses
            .lock()
            .unwrap()
            .insert(buffer.as_ptr() as usize);
        if self.fail_once_at == Some(offset) && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(ferrosa_common::Error::Io(std::io::Error::other(
                "one failed prefetch",
            )));
        }
        self.data.as_slice().read_at(buffer, offset)
    }
    fn len(&self) -> Result<u64> {
        Ok(self.data.len() as u64)
    }
}
fn buffer_probe(fail_once_at: Option<u64>) -> (BufferProbe, Arc<Vec<u8>>, BufferAddresses) {
    let data = Arc::new((0..32 * 4096).map(|n| (n % 251) as u8).collect::<Vec<_>>());
    let addresses = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    (
        BufferProbe {
            data: Arc::clone(&data),
            addresses: Arc::clone(&addresses),
            fail_once_at,
            failed: std::sync::atomic::AtomicBool::new(false),
        },
        data,
        addresses,
    )
}

#[test]
fn backpressure_read_ahead_reuses_two_open_time_buffers_for_sequential_and_random_reads() {
    let (source, expected, addresses) = buffer_probe(None);
    let reader = ReadAheadReader::with_prefetch(source, 4096).unwrap();
    assert_eq!(
        reader.state.lock().unwrap().window.data.capacity(),
        4096,
        "current window must be allocated at open"
    );
    let mut buffer = [0; 4096];
    for window in 0..32 {
        assert_eq!(
            reader.read_at(&mut buffer, (window * 4096) as u64).unwrap(),
            buffer.len()
        );
        assert_eq!(&buffer, &expected[window * 4096..(window + 1) * 4096]);
    }
    for offset in [0, 8192, 2048, 7000, 128000, 0, 4097, 12288] {
        let n = reader.read_at(&mut buffer[..97], offset).unwrap();
        assert_eq!(
            &buffer[..n],
            &expected[offset as usize..offset as usize + n]
        );
    }
    drop(reader);
    assert_eq!(
        addresses.lock().unwrap().len(),
        2,
        "every window must use one of the same two allocations"
    );
}

#[test]
fn backpressure_read_ahead_returns_failed_prefetch_buffer_for_retry() {
    let (source, expected, addresses) = buffer_probe(Some(4096));
    let reader = ReadAheadReader::with_prefetch(source, 4096).unwrap();
    let mut buffer = [0; 4096];
    reader.read_at(&mut buffer, 0).unwrap();
    assert!(reader
        .read_at(&mut buffer, 4096)
        .unwrap_err()
        .to_string()
        .contains("one failed prefetch"));
    for offset in [4096, 8192, 0, 4096] {
        reader.read_at(&mut buffer, offset).unwrap();
        assert_eq!(
            &buffer,
            &expected[offset as usize..offset as usize + buffer.len()]
        );
    }
    drop(reader);
    assert_eq!(
        addresses.lock().unwrap().len(),
        2,
        "failure must return the payload allocation"
    );
}

#[test]
fn backpressure_read_ahead_cancelled_prefetch_drops_both_buffers_and_joins() {
    struct CancelledSource {
        inner: BufferProbe,
        entered: crossbeam_channel::Sender<()>,
        cancelled: crossbeam_channel::Receiver<()>,
    }
    impl ReadAt for CancelledSource {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
            if offset != 0 {
                self.inner
                    .addresses
                    .lock()
                    .unwrap()
                    .insert(buffer.as_ptr() as usize);
                self.entered.send(()).unwrap();
                assert_eq!(
                    self.cancelled
                        .recv_timeout(std::time::Duration::from_secs(5)),
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected)
                );
                return Err(ferrosa_common::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "controlled read cancelled",
                )));
            }
            self.inner.read_at(buffer, offset)
        }
        fn len(&self) -> Result<u64> {
            self.inner.len()
        }
    }
    let (source, data, addresses) = buffer_probe(None);
    let cancel = ferrosa_common::CancelToken::new();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let reader = ReadAheadReader::with_prefetch(
        CancelledSource {
            inner: source,
            entered: entered_tx,
            cancelled: cancel.closed(),
        },
        4096,
    )
    .unwrap();
    let mut buffer = [0; 4096];
    reader.read_at(&mut buffer, 0).unwrap();
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    let dropping = std::thread::spawn(move || {
        drop(reader);
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx.try_recv().is_err(),
        "drop must join the outstanding read"
    );
    cancel.cancel(ferrosa_common::CancelReason::Shutdown);
    done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    dropping.join().unwrap();
    assert_eq!(
        Arc::strong_count(&data),
        1,
        "reader and worker must release source ownership"
    );
    assert_eq!(addresses.lock().unwrap().len(), 2);
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "construct the existing read-ahead transport with a disconnected peer to test owned-buffer return"
)]
fn backpressure_read_ahead_failed_request_returns_its_owned_spare() {
    let (requests, receiver) = channel();
    drop(receiver);
    let (_response_tx, responses) = channel();
    let worker = Worker {
        requests: Some(requests),
        responses,
        handle: None,
    };
    let buffer = Vec::with_capacity(4096);
    let address = buffer.as_ptr();
    let (error, returned) = worker.request(4096, buffer).unwrap_err();
    assert!(error.to_string().contains("worker exited"));
    assert_eq!(returned.as_ptr(), address);
    assert_eq!(returned.capacity(), 4096);
}
