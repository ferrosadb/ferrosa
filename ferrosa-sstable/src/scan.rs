//! Module: Windowed, read-ahead positional reader for sequential SSTable scans.
//! Correctness: Correct when every `read_at` returns exactly the bytes the inner
//!   reader would (any offset, any length, EOF included), the number of inner reads
//!   for a sequential scan is bounded by `ceil(len / window)`, at most one inner read
//!   is ever in flight per reader, memory held is bounded by two windows (current +
//!   one prefetch), and an inner failure — foreground or on the prefetch thread —
//!   reaches the caller as an error.
//! Last revised: 2026-09-26
//! Last changed: New module — compaction input reads were one small `pread` per
//!   compression chunk with no read-ahead (CASSANDRA-15452), which is latency-bound
//!   on disaggregated storage.

use crate::io::ReadAt;
use ferrosa_common::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn eof_error(at: u64, len: u64) -> ferrosa_common::Error {
    ferrosa_common::Error::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("read-ahead: source ended at {at} but reported length {len}"),
    ))
}

/// Read `[pos, pos + min(window, len - pos))` from `inner` into `buf` (resized).
/// A source that ends early is `UnexpectedEof`, never a silently short window.
fn read_window<R: ReadAt + ?Sized>(
    inner: &R,
    len: u64,
    window: usize,
    pos: u64,
    mut buf: Vec<u8>,
) -> Result<Vec<u8>> {
    let want = (len - pos).min(window as u64) as usize;
    buf.resize(want, 0);
    let mut filled = 0;
    while filled < want {
        let n = inner.read_at(&mut buf[filled..], pos + filled as u64)?;
        if n == 0 {
            return Err(eof_error(pos + filled as u64, len));
        }
        filled += n;
    }
    Ok(buf)
}

/// The bytes currently buffered: `data` holds file bytes `[start, start + data.len())`.
struct Window {
    start: u64,
    data: Vec<u8>,
}

impl Window {
    fn slice_from(&self, pos: u64) -> Option<&[u8]> {
        let end = self.start + self.data.len() as u64;
        (pos >= self.start && pos < end).then(|| &self.data[(pos - self.start) as usize..])
    }
}

type Fetched = (u64, Result<Vec<u8>>);

/// One background thread that reads a requested window and sends it back. It runs
/// one request at a time, and the reader issues one request at a time, so at most
/// one prefetch is ever outstanding.
struct Worker {
    requests: Option<Sender<u64>>,
    responses: Receiver<Fetched>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn spawn<R: ReadAt + Send + Sync + 'static>(
        inner: Arc<R>,
        len: u64,
        window: usize,
    ) -> Result<Self> {
        let (req_tx, req_rx) = channel::<u64>();
        let (resp_tx, resp_rx) = channel::<Fetched>();
        let handle = std::thread::Builder::new()
            .name("sstable-readahead".into())
            .spawn(move || {
                while let Ok(pos) = req_rx.recv() {
                    let fetched = read_window(&*inner, len, window, pos, Vec::new());
                    if resp_tx.send((pos, fetched)).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            requests: Some(req_tx),
            responses: resp_rx,
            handle: Some(handle),
        })
    }

    fn request(&self, pos: u64) -> Result<()> {
        self.requests
            .as_ref()
            .expect("requests sender present until drop")
            .send(pos)
            .map_err(|_| worker_gone())
    }

    fn take_response(&self) -> Result<Fetched> {
        self.responses.recv().map_err(|_| worker_gone())
    }
}

fn worker_gone() -> ferrosa_common::Error {
    ferrosa_common::Error::Io(std::io::Error::other(
        "read-ahead worker exited unexpectedly",
    ))
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Closing the request channel ends the worker loop after its current read.
        drop(self.requests.take());
        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                tracing::error!("read-ahead worker panicked");
            }
        }
    }
}

struct State {
    window: Window,
    /// Start offset of the window the worker is currently reading, if any.
    pending: Option<u64>,
    worker: Option<Worker>,
}

/// Wraps a [`ReadAt`] with a single bounded window so a sequential scan issues one
/// large inner read per window instead of one per caller read.
///
/// The wrapped file must be immutable (SSTable components are): `len` is read once
/// at construction. A source that returns fewer bytes than its own `len()` promised
/// is reported as `UnexpectedEof`, never padded or truncated silently.
pub struct ReadAheadReader<R: ReadAt> {
    inner: Arc<R>,
    len: u64,
    window: usize,
    state: Mutex<State>,
    prefetch_hits: AtomicU64,
}

impl<R: ReadAt> ReadAheadReader<R> {
    /// Build a reader whose window is `window` bytes. `window` must be non-zero.
    pub fn new(inner: R, window: usize) -> Result<Self> {
        Self::build(inner, window, |_, _, _| Ok(None))
    }

    fn build(
        inner: R,
        window: usize,
        make_worker: impl FnOnce(&Arc<R>, u64, usize) -> Result<Option<Worker>>,
    ) -> Result<Self> {
        if window == 0 {
            return Err(ferrosa_common::Error::InvalidData(
                "ReadAheadReader window must be non-zero".into(),
            ));
        }
        let len = inner.len()?;
        let inner = Arc::new(inner);
        let worker = make_worker(&inner, len, window)?;
        Ok(Self {
            inner,
            len,
            window,
            state: Mutex::new(State {
                window: Window {
                    start: 0,
                    data: Vec::new(),
                },
                pending: None,
                worker,
            }),
            prefetch_hits: AtomicU64::new(0),
        })
    }

    /// Windows served from a prefetch instead of a foreground read.
    pub fn prefetch_hits(&self) -> u64 {
        self.prefetch_hits.load(Ordering::Relaxed)
    }

    /// Make `state.window` start at `pos` (`pos < len`), from the outstanding
    /// prefetch when it is for `pos`, otherwise with a foreground read; then start
    /// prefetching the window after it.
    fn refill(&self, state: &mut State, pos: u64) -> Result<()> {
        let reusable = std::mem::take(&mut state.window.data);
        let data = match (state.pending.take(), state.worker.as_ref()) {
            (Some(p), Some(worker)) if p == pos => {
                self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                worker.take_response()?.1?
            }
            (Some(_), Some(worker)) => {
                // Non-sequential access: the prefetch is for a different window.
                // Drain it so at most one read is ever in flight, then read here.
                let _discarded = worker.take_response()?;
                read_window(&*self.inner, self.len, self.window, pos, reusable)?
            }
            _ => read_window(&*self.inner, self.len, self.window, pos, reusable)?,
        };
        let next = pos + data.len() as u64;
        state.window = Window { start: pos, data };
        if let Some(worker) = state.worker.as_ref() {
            if next < self.len {
                worker.request(next)?;
                state.pending = Some(next);
            }
        }
        Ok(())
    }
}

impl<R: ReadAt + Send + Sync + 'static> ReadAheadReader<R> {
    /// Like [`Self::new`], plus a background read of the *next* window while the
    /// caller consumes the current one. Holds at most two windows in memory.
    pub fn with_prefetch(inner: R, window: usize) -> Result<Self> {
        Self::build(inner, window, |inner, len, window| {
            Worker::spawn(Arc::clone(inner), len, window).map(Some)
        })
    }
}

impl<R: ReadAt> ReadAt for ReadAheadReader<R> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if buf.is_empty() || offset >= self.len {
            return Ok(0);
        }
        let target = buf.len().min((self.len - offset) as usize);
        let mut state = self.state.lock().expect("read-ahead state poisoned");
        let mut copied = 0;
        while copied < target {
            let pos = offset + copied as u64;
            if state.window.slice_from(pos).is_none() {
                self.refill(&mut state, pos)?;
            }
            let avail = state
                .window
                .slice_from(pos)
                .expect("window covers pos after refill");
            let n = avail.len().min(target - copied);
            buf[copied..copied + n].copy_from_slice(&avail[..n]);
            copied += n;
        }
        Ok(copied)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.len)
    }
}

/// Default read-ahead window for compaction input scans.
pub const DEFAULT_SCAN_WINDOW: usize = 1024 * 1024;

/// Largest accepted window; bounds per-input memory (two windows) so a typo cannot
/// allocate gigabytes per input.
pub const MAX_SCAN_WINDOW: usize = 256 * 1024 * 1024;

/// Parse `FERROSA_COMPACTION_READAHEAD_BYTES`. Absent ⇒ [`DEFAULT_SCAN_WINDOW`].
/// The result is rounded up to a whole [`crate::direct::BLOCK`]. Unparseable, zero
/// or oversized values are an `Err` describing the problem — the caller must log it
/// and choose the default visibly, never silently.
pub fn parse_scan_window(value: Option<&str>) -> std::result::Result<usize, String> {
    let Some(raw) = value else {
        return Ok(DEFAULT_SCAN_WINDOW);
    };
    let bytes: usize = raw
        .trim()
        .parse()
        .map_err(|e| format!("read-ahead window {raw:?} is not a byte count: {e}"))?;
    if bytes == 0 {
        return Err("read-ahead window must be non-zero".into());
    }
    if bytes > MAX_SCAN_WINDOW {
        return Err(format!(
            "read-ahead window {bytes} exceeds the {MAX_SCAN_WINDOW}-byte cap"
        ));
    }
    Ok(bytes.next_multiple_of(crate::direct::BLOCK))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Oracle-backed source that counts how it is called.
    struct CountingSource {
        data: Vec<u8>,
        reads: Arc<AtomicU64>,
        largest: Arc<AtomicU64>,
    }

    impl CountingSource {
        fn new(len: usize) -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
            let reads = Arc::new(AtomicU64::new(0));
            let largest = Arc::new(AtomicU64::new(0));
            let data = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            (
                Self {
                    data,
                    reads: Arc::clone(&reads),
                    largest: Arc::clone(&largest),
                },
                reads,
                largest,
            )
        }
    }

    impl ReadAt for CountingSource {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.largest.fetch_max(buf.len() as u64, Ordering::SeqCst);
            self.data.as_slice().read_at(buf, offset)
        }

        fn len(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    #[test]
    fn scan_window_parsing_is_strict_and_block_aligned() {
        assert_eq!(parse_scan_window(None), Ok(DEFAULT_SCAN_WINDOW));
        assert_eq!(parse_scan_window(Some("4194304")), Ok(4 * 1024 * 1024));
        assert_eq!(parse_scan_window(Some(" 8192 ")), Ok(8192));
        assert_eq!(
            parse_scan_window(Some("1000")),
            Ok(4096),
            "rounds up to a block"
        );
        assert!(parse_scan_window(Some("0")).is_err());
        assert!(parse_scan_window(Some("lots")).is_err());
        assert!(parse_scan_window(Some("-5")).is_err());
        assert!(parse_scan_window(Some(&(MAX_SCAN_WINDOW + 1).to_string())).is_err());
        assert_eq!(
            parse_scan_window(Some(&MAX_SCAN_WINDOW.to_string())),
            Ok(MAX_SCAN_WINDOW)
        );
    }

    #[test]
    fn zero_window_is_rejected() {
        let (src, _, _) = CountingSource::new(10);
        assert!(ReadAheadReader::new(src, 0).is_err());
    }

    #[test]
    fn sequential_scan_is_byte_exact_with_one_inner_read_per_window() {
        let (src, reads, _) = CountingSource::new(10_000);
        let oracle = src.data.clone();
        let r = ReadAheadReader::new(src, 1024).expect("new");
        let mut out = Vec::new();
        let mut buf = [0u8; 100];
        let mut off = 0u64;
        loop {
            let n = r.read_at(&mut buf, off).expect("read");
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            off += n as u64;
        }
        assert_eq!(out, oracle);
        // ceil(10_000 / 1024) = 10 windows; the final EOF probe is served from the
        // already-known length and must not hit the source again.
        assert_eq!(reads.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn inner_reads_never_exceed_the_window() {
        let (src, _, largest) = CountingSource::new(5_000);
        let r = ReadAheadReader::new(src, 512).expect("new");
        let mut big = vec![0u8; 4_000];
        // A caller read larger than the window is still served, in window steps.
        assert_eq!(r.read_at(&mut big, 0).expect("read"), 4_000);
        assert!(largest.load(Ordering::SeqCst) <= 512);
    }

    #[test]
    fn eof_semantics_match_the_inner_reader() {
        let (src, _, _) = CountingSource::new(1_000);
        let oracle = src.data.clone();
        let r = ReadAheadReader::new(src, 256).expect("new");
        let mut buf = [0u8; 64];
        assert_eq!(r.read_at(&mut buf, 990).expect("tail"), 10);
        assert_eq!(&buf[..10], &oracle[990..]);
        assert_eq!(r.read_at(&mut buf, 1_000).expect("at eof"), 0);
        assert_eq!(r.read_at(&mut buf, 5_000).expect("past eof"), 0);
        assert_eq!(r.read_at(&mut [], 10).expect("empty buf"), 0);
        assert_eq!(r.len().expect("len"), 1_000);
    }

    /// Source that tracks concurrent in-flight reads and can be told to fail.
    struct ProbeSource {
        data: Vec<u8>,
        in_flight: Arc<AtomicU64>,
        max_in_flight: Arc<AtomicU64>,
        fail_at_or_after: Option<u64>,
    }

    impl ReadAt for ProbeSource {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            let out = match self.fail_at_or_after {
                Some(limit) if offset >= limit => Err(ferrosa_common::Error::Io(
                    std::io::Error::other("injected source failure"),
                )),
                _ => self.data.as_slice().read_at(buf, offset),
            };
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            out
        }

        fn len(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    fn probe(len: usize, fail_at: Option<u64>) -> (ProbeSource, Arc<AtomicU64>) {
        let max = Arc::new(AtomicU64::new(0));
        let src = ProbeSource {
            data: (0..len).map(|i| (i * 17 % 253) as u8).collect(),
            in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight: Arc::clone(&max),
            fail_at_or_after: fail_at,
        };
        (src, max)
    }

    fn scan_all<R: ReadAt>(r: &R, step: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; step];
        let mut off = 0u64;
        loop {
            let n = r.read_at(&mut buf, off)?;
            if n == 0 {
                return Ok(out);
            }
            out.extend_from_slice(&buf[..n]);
            off += n as u64;
        }
    }

    #[test]
    fn prefetch_serves_every_window_after_the_first() {
        let (src, _) = probe(10_000, None);
        let oracle = src.data.clone();
        let r = ReadAheadReader::with_prefetch(src, 1024).expect("new");
        assert_eq!(scan_all(&r, 100).expect("scan"), oracle);
        // 10 windows: window 0 is a foreground read, windows 1..=9 were prefetched.
        assert_eq!(r.prefetch_hits(), 9);
    }

    #[test]
    fn at_most_one_read_is_ever_in_flight() {
        let (src, max_in_flight) = probe(20_000, None);
        let r = ReadAheadReader::with_prefetch(src, 512).expect("new");
        scan_all(&r, 300).expect("scan");
        assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_failure_on_the_prefetch_thread_reaches_the_caller() {
        let (src, _) = probe(4_096, Some(1_024));
        let r = ReadAheadReader::with_prefetch(src, 1_024).expect("new");
        let err = scan_all(&r, 128).expect_err("second window must fail loudly");
        assert!(err.to_string().contains("injected source failure"), "{err}");
    }

    #[test]
    fn dropping_the_reader_stops_the_worker_and_releases_the_source() {
        let (src, _) = probe(8_192, None);
        let shared = Arc::new(src);
        struct Shared(Arc<ProbeSource>);
        impl ReadAt for Shared {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
                self.0.read_at(buf, offset)
            }
            fn len(&self) -> Result<u64> {
                self.0.len()
            }
        }
        let r = ReadAheadReader::with_prefetch(Shared(Arc::clone(&shared)), 1_024).expect("new");
        let mut buf = [0u8; 10];
        r.read_at(&mut buf, 0).expect("read"); // leaves a prefetch outstanding
        drop(r);
        assert_eq!(Arc::strong_count(&shared), 1, "worker must have exited");
    }

    proptest! {
        #[test]
        fn prefetching_reader_matches_the_oracle_for_any_access_pattern(
            len in 0usize..6_000,
            window in 1usize..1_024,
            reads in prop::collection::vec((0u64..7_000, 0usize..2_000), 1..30),
        ) {
            let (src, _) = probe(len, None);
            let oracle = src.data.clone();
            let r = ReadAheadReader::with_prefetch(src, window).expect("new");
            for (off, want) in reads {
                let mut got = vec![0u8; want];
                let n = r.read_at(&mut got, off).expect("read");
                let mut expect = vec![0u8; want];
                let m = oracle.as_slice().read_at(&mut expect, off).expect("oracle");
                prop_assert_eq!(n, m);
                prop_assert_eq!(&got[..n], &expect[..m]);
            }
        }
    }

    proptest! {
        #[test]
        fn any_read_matches_the_oracle(
            len in 0usize..6_000,
            window in 1usize..2_048,
            reads in prop::collection::vec((0u64..7_000, 0usize..3_000), 1..30),
        ) {
            let (src, _, _) = CountingSource::new(len);
            let oracle = src.data.clone();
            let r = ReadAheadReader::new(src, window).expect("new");
            for (off, want) in reads {
                let mut got = vec![0u8; want];
                let n = r.read_at(&mut got, off).expect("read");
                let mut expect = vec![0u8; want];
                let m = oracle.as_slice().read_at(&mut expect, off).expect("oracle");
                prop_assert_eq!(n, m);
                prop_assert_eq!(&got[..n], &expect[..m]);
            }
        }
    }
}
