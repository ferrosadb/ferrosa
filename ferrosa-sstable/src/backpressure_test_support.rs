//! Bounded, permit-gated device wrapper for cross-crate backpressure tests.
//!
//! No payload is retained. A single fixed state records attempts/completions;
//! all waits have deadlines. This module is absent from production builds.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use ferrosa_common::{Error, Result};

use crate::direct::DirectMode;
use crate::pump::SegmentSink;

#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
    /// Device calls that reached the gate (parked or admitted).
    pub attempted: u64,
    /// Device calls past the gate (permit consumed, or gate open/failed):
    /// `admitted - completed` is the number IN FLIGHT, and
    /// `attempted - admitted` the number PARKED awaiting a permit.
    pub admitted: u64,
    pub completed: u64,
    pub bytes: u64,
}

#[derive(Default)]
struct State {
    progress: Progress,
    permits: u64,
    open: bool,
    failed: bool,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    timeout: Duration,
}

/// Test controller; dropping it releases all writes so a failed assertion
/// cannot strand the pump's flusher during unwinding.
pub struct WriteGate(Arc<Shared>);

impl WriteGate {
    pub fn new(timeout: Duration) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            timeout,
        }))
    }

    pub fn wrap(&self, sink: Box<dyn SegmentSink>) -> Box<dyn SegmentSink> {
        Box::new(GatedSink {
            inner: sink,
            shared: Arc::clone(&self.0),
        })
    }

    /// Gate a non-write stage (codec/readback/read-at) without retaining bytes.
    pub fn checkpoint(&self) -> Result<()> {
        self.0.admit()?;
        self.0.completed(0);
        Ok(())
    }

    pub fn progress(&self) -> Progress {
        self.0.state.lock().unwrap().progress
    }

    /// Wait for an attempted device operation, before it receives a permit.
    pub fn wait_for_attempts(&self, count: u64) {
        let deadline = Instant::now() + self.0.timeout;
        let mut state = self.0.state.lock().unwrap();
        while state.progress.attempted < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (next, result) = self.0.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(
                !result.timed_out() || state.progress.attempted >= count,
                "device never reached attempt {count}: {:?}",
                state.progress
            );
        }
    }

    /// Wait until no admitted device call is still running
    /// (`admitted == completed`). Calls PARKED awaiting a permit do not count:
    /// they need a permit to move, so waiting on them would hang, while an
    /// admitted call always finishes on its own. This is the "no device call is
    /// in flight" precondition a phase boundary needs before the next phase's
    /// "nothing may complete while gated" window opens.
    pub fn wait_for_no_inflight(&self) {
        let deadline = Instant::now() + self.0.timeout;
        let mut state = self.0.state.lock().unwrap();
        while state.progress.admitted != state.progress.completed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (next, result) = self.0.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(
                !result.timed_out() || state.progress.admitted == state.progress.completed,
                "admitted device call never completed: {:?}",
                state.progress
            );
        }
    }

    /// Wait until `count` device calls have been admitted (consumed a permit).
    pub fn wait_for_admitted(&self, count: u64) {
        let deadline = Instant::now() + self.0.timeout;
        let mut state = self.0.state.lock().unwrap();
        while state.progress.admitted < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (next, result) = self.0.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(
                !result.timed_out() || state.progress.admitted >= count,
                "device never admitted call {count}: {:?}",
                state.progress
            );
        }
    }

    /// Release ONE permit only if a device call is parked awaiting one that no
    /// outstanding permit already covers. Returns the `admitted` count that call
    /// will reach once it consumes the permit, or `None` if nothing was parked.
    ///
    /// Unlike `release`, this can never bank a surplus permit: every permit it
    /// grants has a parked call that will consume it at once.
    pub fn release_if_parked(&self) -> Option<u64> {
        let mut state = self.0.state.lock().unwrap();
        let parked = state
            .progress
            .attempted
            .saturating_sub(state.progress.admitted);
        if parked <= state.permits {
            return None;
        }
        state.permits += 1;
        let target = state.progress.admitted + state.permits;
        drop(state);
        self.0.changed.notify_all();
        Some(target)
    }

    /// One permit admits one device call (which may contain coalesced segments).
    pub fn release(&self, count: u64) {
        self.0.state.lock().unwrap().permits += count;
        self.0.changed.notify_all();
    }

    pub fn open(&self) {
        self.0.state.lock().unwrap().open = true;
        self.0.changed.notify_all();
    }

    pub fn fail(&self) {
        self.0.state.lock().unwrap().failed = true;
        self.0.changed.notify_all();
    }
}

impl Drop for WriteGate {
    fn drop(&mut self) {
        self.open();
    }
}

struct GatedSink {
    inner: Box<dyn SegmentSink>,
    shared: Arc<Shared>,
}

impl Shared {
    fn admit(&self) -> Result<()> {
        let deadline = Instant::now() + self.timeout;
        let mut state = self.state.lock().unwrap();
        state.progress.attempted += 1;
        self.changed.notify_all();
        while !state.open && !state.failed && state.permits == 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (next, waited) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if waited.timed_out() && !state.open && !state.failed && state.permits == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "backpressure test gate timed out",
                )));
            }
        }
        if state.failed {
            return Err(Error::Io(std::io::Error::other(
                "backpressure test device failure",
            )));
        }
        if !state.open {
            state.permits -= 1;
        }
        state.progress.admitted += 1;
        Ok(())
    }

    fn completed(&self, bytes: usize) {
        let mut state = self.state.lock().unwrap();
        state.progress.completed += 1;
        state.progress.bytes += bytes as u64;
        self.changed.notify_all();
    }
}

impl SegmentSink for GatedSink {
    fn pwrite(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        self.shared.admit()?;
        self.inner.pwrite(buf, offset)?;
        self.shared.completed(buf.len());
        Ok(())
    }
    fn pwritev(&mut self, bufs: &[&[u8]], offset: u64) -> Result<()> {
        self.shared.admit()?;
        self.inner.pwritev(bufs, offset)?;
        self.shared
            .completed(bufs.iter().map(|buf| buf.len()).sum());
        Ok(())
    }
    fn pwrite_buffers(&mut self, buffers: &[crate::pump::PumpBuffer], offset: u64) -> Result<()> {
        self.shared.admit()?;
        self.inner.pwrite_buffers(buffers, offset)?;
        self.shared
            .completed(buffers.iter().map(crate::pump::PumpBuffer::len).sum());
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        self.inner.sync_data()
    }
    fn set_len(&mut self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }
    fn fadvise_dontneed(&mut self) -> Result<()> {
        self.inner.fadvise_dontneed()
    }
    fn mode(&self) -> DirectMode {
        self.inner.mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pump::test_support::RecordingSink;
    use crate::pump::{AlignedPump, NeverAbort};
    use std::path::PathBuf;

    const BLOCK: usize = 4096;
    const DEADLINE: Duration = Duration::from_secs(5);

    #[test]
    fn backpressure_b4_ring_stops_at_owned_segment_bound_and_resumes() {
        for depth in 1..=4 {
            let gate = WriteGate::new(DEADLINE);
            let (sink, recorded) = RecordingSink::new(DirectMode::Direct);
            let mut pump = AlignedPump::open_with_depth(
                gate.wrap(Box::new(sink)),
                BLOCK,
                BLOCK,
                PathBuf::from("Data.db"),
                depth,
                Arc::new(NeverAbort::new()),
            );
            let segments = depth + 3;
            let (done_tx, done_rx) = crossbeam_channel::bounded(segments);
            let worker = std::thread::spawn(move || {
                for sequence in 0..segments {
                    pump.write_all(&[sequence as u8; BLOCK]).unwrap();
                    done_tx.send(sequence).unwrap();
                }
                pump.finish().unwrap()
            });
            gate.wait_for_attempts(1);
            for sequence in 0..=depth {
                assert_eq!(done_rx.recv_timeout(DEADLINE).unwrap(), sequence);
            }
            assert_eq!(gate.progress().completed, 0);
            assert_eq!(
                done_rx.recv_timeout(Duration::from_millis(25)),
                Err(crossbeam_channel::RecvTimeoutError::Timeout)
            );
            // One device permit returns at least one segment. The flusher may
            // coalesce several queued segments in the same operation.
            gate.release(1);
            assert_eq!(done_rx.recv_timeout(DEADLINE).unwrap(), depth + 1);
            gate.wait_for_attempts(2);
            assert_eq!(gate.progress().completed, 1);
            assert!(gate.progress().bytes <= ((depth + 1) * BLOCK) as u64);
            gate.open();
            assert_eq!(done_rx.recv_timeout(DEADLINE).unwrap(), depth + 2);
            assert_eq!(worker.join().unwrap(), (segments * BLOCK) as u64);
            let expected: Vec<u8> = (0..segments).flat_map(|n| [n as u8; BLOCK]).collect();
            assert_eq!(recorded.bytes(), expected);
        }
    }

    #[test]
    fn backpressure_b6_b7_depth_zero_blocks_each_write_without_queue() {
        for component in [
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
            "CRC.db",
        ] {
            let gate = WriteGate::new(DEADLINE);
            let (sink, recorded) = RecordingSink::new(DirectMode::Direct);
            let mut pump = AlignedPump::open(
                gate.wrap(Box::new(sink)),
                BLOCK,
                BLOCK,
                PathBuf::from(component),
            );
            let (tx, rx) = crossbeam_channel::bounded(2);
            let worker = std::thread::spawn(move || {
                pump.write_all(&[17; BLOCK]).unwrap();
                tx.send(1).unwrap();
                pump.write_all(&[29; BLOCK]).unwrap();
                tx.send(2).unwrap();
                pump.finish().unwrap()
            });
            for operation in 1..=2 {
                gate.wait_for_attempts(operation);
                assert_eq!(gate.progress().completed, operation - 1);
                assert_eq!(rx.try_recv(), Err(crossbeam_channel::TryRecvError::Empty));
                gate.release(1);
                assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), operation);
            }
            assert_eq!(worker.join().unwrap(), (2 * BLOCK) as u64);
            assert_eq!(recorded.bytes(), [[17; BLOCK], [29; BLOCK]].concat());
        }
    }

    #[test]
    fn backpressure_b5_header_patch_waits_and_follows_all_offset_writes() {
        let gate = WriteGate::new(DEADLINE);
        let (sink, recorded) = RecordingSink::new(DirectMode::Direct);
        let mut pump = AlignedPump::open(
            gate.wrap(Box::new(sink)),
            BLOCK,
            BLOCK,
            PathBuf::from("CompressionInfo.db"),
        );
        let (tx, rx) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(move || {
            pump.write_all(&[0; BLOCK]).unwrap();
            pump.write_all(&[7; BLOCK]).unwrap();
            let result = pump.finish_with_patched_header(&[42; BLOCK]);
            tx.send(result).unwrap();
        });
        for operation in 1..=3 {
            gate.wait_for_attempts(operation);
            assert_eq!(gate.progress().completed, operation - 1);
            assert!(
                rx.try_recv().is_err(),
                "finish returned before its header write"
            );
            gate.release(1);
        }
        assert_eq!(
            rx.recv_timeout(DEADLINE).unwrap().unwrap(),
            (2 * BLOCK) as u64
        );
        worker.join().unwrap();
        assert_eq!(
            recorded
                .writes()
                .iter()
                .map(|w| w.offset)
                .collect::<Vec<_>>(),
            vec![0, BLOCK as u64, 0]
        );
        assert_eq!(recorded.bytes(), [[42; BLOCK], [7; BLOCK]].concat());
    }

    #[test]
    fn backpressure_b4_device_failure_unblocks_producer_and_joins_flusher() {
        let gate = WriteGate::new(DEADLINE);
        let (sink, recorded) = RecordingSink::new(DirectMode::Direct);
        let mut pump = AlignedPump::open_with_depth(
            gate.wrap(Box::new(sink)),
            BLOCK,
            BLOCK,
            PathBuf::from("Data.db"),
            1,
            Arc::new(NeverAbort::new()),
        );
        let (tx, rx) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(move || {
            let result = pump.write_all(&[3; BLOCK * 4]);
            drop(pump); // Completion includes the flusher join.
            tx.send(result).unwrap();
        });
        gate.wait_for_attempts(1);
        assert!(rx.try_recv().is_err());
        gate.fail();
        let error = rx.recv_timeout(DEADLINE).unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("backpressure test device failure"),
            "{error}"
        );
        worker.join().unwrap();
        assert_eq!(gate.progress().completed, 0);
        assert!(recorded.bytes().is_empty());
    }
}

#[cfg(test)]
mod slow {
    use super::*;
    use crate::pump::test_support::RecordingSink;
    use crate::pump::{AlignedPump, NeverAbort};
    use std::path::PathBuf;

    /// E5 duration coverage; kept in `::slow::` so fast CI excludes the
    /// deliberate minute-long stall. There is no sleep or polling loop.
    #[test]
    fn backpressure_e5_minute_stall_resumes_without_loss_or_reordering() {
        const BLOCK: usize = 4096;
        const DEADLINE: Duration = Duration::from_secs(75);
        let gate = WriteGate::new(DEADLINE);
        let (sink, recorded) = RecordingSink::new(DirectMode::Direct);
        let mut pump = AlignedPump::open_with_depth(
            gate.wrap(Box::new(sink)),
            BLOCK,
            BLOCK,
            PathBuf::from("long-stall-Data.db"),
            1,
            Arc::new(NeverAbort::new()),
        );
        let (tx, rx) = crossbeam_channel::bounded(8);
        let worker = std::thread::spawn(move || {
            for n in 0..8 {
                pump.write_all(&[n; BLOCK]).unwrap();
                tx.send(n).unwrap();
            }
            pump.finish().unwrap()
        });
        gate.wait_for_attempts(1);
        assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), 0);
        assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), 1);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(60)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
        );
        assert_eq!(gate.progress().attempted, 1);
        assert_eq!(gate.progress().completed, 0);
        gate.release(1);
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
        gate.open();
        for n in 3..8 {
            assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), n);
        }
        assert_eq!(worker.join().unwrap(), (BLOCK * 8) as u64);
        let expected: Vec<_> = (0..8).flat_map(|n| [n; BLOCK]).collect();
        assert_eq!(recorded.bytes(), expected);
        assert_eq!(
            crc32fast::hash(&recorded.bytes()),
            crc32fast::hash(&expected)
        );
    }
}

#[cfg(test)]
mod read_ahead_chain {
    use super::*;
    use crate::io::{FileReadAt, ReadAt};
    use crate::pump::test_support::RecordingSink;
    use crate::pump::{AlignedPump, NeverAbort};
    use crate::scan::ReadAheadReader;
    use std::path::PathBuf;

    struct GateReadAt {
        inner: FileReadAt,
        gate: Arc<WriteGate>,
    }
    impl ReadAt for GateReadAt {
        fn read_at(&self, bytes: &mut [u8], offset: u64) -> Result<usize> {
            self.gate.checkpoint()?;
            self.inner.read_at(bytes, offset)
        }
        fn len(&self) -> Result<u64> {
            self.inner.len()
        }
    }

    /// B1/B4: the real foreground+prefetch reader is pulled by a producer
    /// writing a saturated pump. It cannot run ahead after the producer parks.
    #[test]
    fn backpressure_b1_read_ahead_stops_at_current_plus_one_prefetch() {
        const WINDOW: usize = 4096;
        const DEADLINE: Duration = Duration::from_secs(10);
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("input.db");
        let expected: Vec<u8> = (0..32).flat_map(|n| [n; WINDOW]).collect();
        std::fs::write(&source_path, &expected).unwrap();
        let reads = Arc::new(WriteGate::new(DEADLINE));
        let reader = ReadAheadReader::with_prefetch(
            GateReadAt {
                inner: FileReadAt::open(&source_path).unwrap(),
                gate: Arc::clone(&reads),
            },
            WINDOW,
        )
        .unwrap();
        let writes = WriteGate::new(DEADLINE);
        let (sink, recorded) = RecordingSink::new(crate::direct::DirectMode::Direct);
        let mut pump = AlignedPump::open_with_depth(
            writes.wrap(Box::new(sink)),
            WINDOW,
            WINDOW,
            PathBuf::from("Data.db"),
            1,
            Arc::new(NeverAbort::new()),
        );
        let (tx, rx) = crossbeam_channel::bounded(32);
        let worker = std::thread::spawn(move || {
            let mut buffer = [0; WINDOW];
            for window in 0..32 {
                assert_eq!(
                    reader
                        .read_at(&mut buffer, (window * WINDOW) as u64)
                        .unwrap(),
                    WINDOW
                );
                pump.write_all(&buffer).unwrap();
                tx.send(window).unwrap();
            }
            pump.finish().unwrap()
        });
        reads.wait_for_attempts(1);
        assert_eq!(
            writes.progress().attempted,
            0,
            "closed input gate must prevent output"
        );
        reads.open();
        writes.wait_for_attempts(1);
        assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), 0);
        assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), 1);
        // Two windows are already owned by the pump, the third is the blocked
        // producer's current read window, and exactly one prefetch may follow.
        reads.wait_for_attempts(4);
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(25)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
        );
        assert_eq!(reads.progress().attempted, 4);
        assert_eq!(writes.progress().completed, 0);
        writes.release(1);
        assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), 2);
        writes.open();
        for window in 3..32 {
            assert_eq!(rx.recv_timeout(DEADLINE).unwrap(), window);
        }
        assert_eq!(worker.join().unwrap(), expected.len() as u64);
        assert_eq!(recorded.bytes(), expected);
        assert_eq!(reads.progress().attempted, 32);
    }
}
