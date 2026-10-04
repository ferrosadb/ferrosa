//! t_a594d4ee: `write_pump_inflight_segments` must return to exactly its
//! starting value after every async pump closes, on every exit path, and must
//! never wrap below zero.
//!
//! Observed live 2026-10-03: the native memory cluster exported
//! `ferrosa_sstable_write_pump_inflight_segments` as 18446744073709551508 —
//! a `u64` gauge decremented about 108 times more than it was incremented.
//!
//! The gauge is process-global (one value feeds Prometheus for every pump in
//! the process), so this binary holds only this file's tests and serializes
//! them on [`GAUGE_LOCK`]: nothing else in the process opens a pump while a
//! scenario measures its delta.

#![cfg(feature = "test-support")]

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use ferrosa_sstable::direct::DirectMode;
use ferrosa_sstable::pump::test_support::{Fault, FaultySink, RecordingSink};
use ferrosa_sstable::pump::{write_pump_inflight_segments, AlignedPump, NeverAbort, SegmentSink};

const BLOCK: usize = 4096;
const SEGMENT: usize = 4 * BLOCK;
const DEPTH: usize = 3;

static GAUGE_LOCK: Mutex<()> = Mutex::new(());

fn serialize() -> MutexGuard<'static, ()> {
    GAUGE_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

fn open(sink: Box<dyn SegmentSink>, name: &str) -> AlignedPump {
    AlignedPump::open_with_depth(
        sink,
        BLOCK,
        SEGMENT,
        PathBuf::from(name),
        DEPTH,
        Arc::new(NeverAbort::new()),
    )
}

/// Panics with the wrapped value spelled out, so a red run names the
/// underflow instead of printing an opaque 20-digit number.
fn assert_gauge_is(expected: u64, context: &str) {
    let now = write_pump_inflight_segments();
    assert_eq!(
        now, expected,
        "{context}: write_pump_inflight_segments is {now} (as i64: {}), expected {expected}",
        now as i64
    );
}

#[test]
fn pump_inflight_gauge_returns_to_start_after_small_file_finish() {
    let _g = serialize();
    let start = write_pump_inflight_segments();
    let (sink, _h) = RecordingSink::new(DirectMode::Direct);
    let mut pump = open(Box::new(sink), "gauge-small.db");
    pump.write_all(&[7u8; 100]).expect("write");
    pump.finish().expect("finish");
    assert_gauge_is(start, "small file, finish");
}

#[test]
fn pump_inflight_gauge_returns_to_start_after_multi_segment_finish() {
    let _g = serialize();
    let start = write_pump_inflight_segments();
    let (sink, _h) = RecordingSink::new(DirectMode::Direct);
    let mut pump = open(Box::new(sink), "gauge-multi.db");
    let chunk = [3u8; 1000];
    for _ in 0..(SEGMENT * 10 / chunk.len()) {
        pump.write_all(&chunk).expect("write");
        let now = write_pump_inflight_segments();
        assert!(
            now <= start + (DEPTH as u64 + 1),
            "mid-stream: gauge {now} (as i64: {}) exceeds start {start} + depth+1",
            now as i64
        );
    }
    pump.finish().expect("finish");
    assert_gauge_is(start, "multi-segment file, finish");
}

#[test]
fn pump_inflight_gauge_returns_to_start_after_drop_without_finish() {
    let _g = serialize();
    let start = write_pump_inflight_segments();
    let (sink, _h) = RecordingSink::new(DirectMode::Direct);
    let mut pump = open(Box::new(sink), "gauge-drop.db");
    pump.write_all(&vec![5u8; SEGMENT * 3 + 17]).expect("write");
    drop(pump);
    assert_gauge_is(start, "dropped without finish");
}

#[test]
fn pump_inflight_gauge_returns_to_start_after_flusher_error() {
    let _g = serialize();
    let start = write_pump_inflight_segments();
    let (sink, _h) = FaultySink::new(DirectMode::Direct);
    let sink = sink.at(0, Fault::Eio);
    let mut pump = open(Box::new(sink), "gauge-eio.db");
    let chunk = vec![9u8; SEGMENT];
    // The flusher fails its first write; the producer learns of it on a later
    // send. Either way the pump ends with an error and every segment it handed
    // off must be accounted back.
    let mut failed = false;
    for _ in 0..(DEPTH + 4) {
        if pump.write_all(&chunk).is_err() {
            failed = true;
            break;
        }
    }
    let finished = pump.finish();
    assert!(
        failed || finished.is_err(),
        "an injected EIO on the first flusher write must surface as an error"
    );
    assert_gauge_is(start, "flusher EIO");
}
