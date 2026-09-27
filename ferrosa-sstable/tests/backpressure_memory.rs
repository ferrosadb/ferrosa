//! Isolated process measurement for B4 bounded memory and resumed throughput.
//! Run with --features test-support --test backpressure_memory -- --nocapture.
#![cfg(feature = "test-support")]

use ferrosa_sstable::backpressure_test_support::WriteGate;
use ferrosa_sstable::pump::{AlignedPump, FileSink, NeverAbort};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc_zeroed(layout);
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(pointer, layout);
    }
    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let next = System.realloc(pointer, old, size);
        if !next.is_null() {
            LIVE.fetch_sub(old.size(), Ordering::Relaxed);
            LIVE.fetch_add(size, Ordering::Relaxed);
        }
        next
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn rss_kib() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    assert!(output.status.success());
    std::str::from_utf8(&output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

mod measurements {
    mod slow {
        use super::super::*;
        #[test]
        fn backpressure_memory_plateaus_and_resumed_throughput_is_measured() {
            const SEGMENT: usize = 1024 * 1024;
            const SEGMENTS: usize = 64;
            let dir = tempfile::tempdir().unwrap();
            let payload = Arc::new(vec![0x35; SEGMENT]);
            let run = |name: &str, stalled: bool| {
                let path = dir.path().join(name);
                let gate = WriteGate::new(Duration::from_secs(75));
                if !stalled {
                    gate.open();
                }
                let (sink, block) = FileSink::create(&path).unwrap();
                let mut pump = AlignedPump::open_with_depth(
                    gate.wrap(Box::new(sink)),
                    block,
                    SEGMENT,
                    path.clone(),
                    1,
                    Arc::new(NeverAbort::new()),
                );
                let bytes = Arc::clone(&payload);
                let (progress_tx, progress_rx) = crossbeam_channel::bounded(SEGMENTS);
                let (result_tx, result_rx) = crossbeam_channel::bounded(1);
                // Prime this observer's select storage before counting the plateau.
                let mut selection = crossbeam_channel::Select::new();
                selection.recv(&result_rx);
                assert!(selection.try_select().is_err());
                drop(selection);
                let worker = std::thread::spawn(move || {
                    // The pump moved to this producer thread: initialize its TLS before
                    // measuring, without introducing writes or changing pump behavior.
                    let (_tx, rx) = crossbeam_channel::bounded::<()>(1);
                    let mut selection = crossbeam_channel::Select::new();
                    selection.recv(&rx);
                    assert!(selection.try_select().is_err());
                    drop(selection);
                    let mut resumed = Instant::now();
                    for n in 0..SEGMENTS {
                        pump.write_all(&bytes).unwrap();
                        if n == 2 {
                            resumed = Instant::now();
                        }
                        progress_tx.send(n).unwrap();
                    }
                    let digest = pump.digest();
                    let length = pump.finish().unwrap();
                    result_tx.send((resumed.elapsed(), digest, length)).unwrap();
                });
                let mut rss = [0; 4];
                let mut live = [0; 4];
                if stalled {
                    gate.wait_for_attempts(1);
                    assert_eq!(progress_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 0);
                    assert_eq!(progress_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
                    let _ = rss_kib(); // Initialize ps/command support outside the samples.
                    for i in 0..4 {
                        assert!(matches!(
                            result_rx.recv_timeout(Duration::from_secs(15)),
                            Err(crossbeam_channel::RecvTimeoutError::Timeout)
                        ));
                        rss[i] = rss_kib();
                        live[i] = LIVE.load(Ordering::Relaxed);
                    }
                    assert!(
                        live.iter().all(|&bytes| bytes == live[0]),
                        "live heap grew while blocked: {live:?}"
                    );
                    assert!(
                        *rss.iter().max().unwrap() - *rss.iter().min().unwrap() <= 1024,
                        "RSS grew by more than1MiB while blocked: {rss:?}"
                    );
                    assert_eq!(gate.progress().completed, 0);
                    gate.open();
                }
                let (elapsed, digest, length) =
                    result_rx.recv_timeout(Duration::from_secs(75)).unwrap();
                worker.join().unwrap();
                assert_eq!(length, (SEGMENT * SEGMENTS) as u64);
                assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
                // Read back with one bounded buffer, independently of producer digest.
                use std::io::Read;
                let mut input = std::fs::File::open(path).unwrap();
                let mut buffer = vec![0; SEGMENT];
                let mut actual = crc32fast::Hasher::new();
                loop {
                    let n = input.read(&mut buffer).unwrap();
                    if n == 0 {
                        break;
                    }
                    actual.update(&buffer[..n]);
                }
                assert_eq!(actual.finalize(), digest);
                let mib_per_sec = (SEGMENTS - 3) as f64 / elapsed.as_secs_f64();
                (mib_per_sec, live, rss)
            };
            let (baseline, _, _) = run("baseline-Data.db", false);
            let (resumed, live, rss) = run("gated-Data.db", true);
            let report = format!("BACKPRESSURE_MEASUREMENT baseline_mib_s={baseline:.3} resumed_mib_s={resumed:.3} ratio={:.3} live_bytes={live:?} rss_kib={rss:?}\n", resumed / baseline);
            let report_path = std::env::var_os("FERROSA_BACKPRESSURE_REPORT")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    std::env::temp_dir()
                        .join(format!("ferrosa-backpressure-{}.txt", std::process::id()))
                });
            std::fs::write(&report_path, &report).unwrap();
            println!("{report}report={}", report_path.display());
        }
    }
}
