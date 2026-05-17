//! Idle-timeout watchdog for streaming RPC consumers.
//!
//! Wraps a [`tokio::sync::mpsc::Receiver`] so the consumer aborts only when
//! the producer has been quiet for longer than `idle_timeout`. Each item
//! that arrives — whether real payload or a heartbeat — resets the
//! deadline. Total wall-clock time has no bound; a steady-but-slow
//! producer can run for hours without tripping the watchdog.
//!
//! This is the building block referenced by [ADR-020] for streaming
//! internode range reads. The handler emits `RangeReadChunk` frames and
//! interleaves `RangeReadHeartbeat` whenever the next chunk takes longer
//! than `heartbeat_interval` to produce. The coordinator wraps its
//! receiver in [`IdleTimeoutWatchdog`] and treats `IdleTimeoutElapsed` as
//! "peer is genuinely stuck — abort and surface a partial result".
//!
//! [ADR-020]: https://github.com/ferrosadb/ferrosa/blob/main/specs/decisions/020-streaming-internode-range-read.md

use std::fmt;
use std::time::Duration;
use tokio::sync::mpsc;

/// The watchdog fired: no activity from the producer within
/// `idle_timeout`. The wrapped receiver is not closed by this error —
/// callers may drop it (to signal cancellation) or keep waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdleTimeoutElapsed {
    /// The configured idle timeout that fired.
    pub idle_timeout: Duration,
}

impl fmt::Display for IdleTimeoutElapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "stream idle timeout elapsed: no activity for {:?}",
            self.idle_timeout
        )
    }
}

impl std::error::Error for IdleTimeoutElapsed {}

/// Wraps a receiver with a per-message idle deadline that resets on each
/// arrival.
pub struct IdleTimeoutWatchdog<T> {
    rx: mpsc::Receiver<T>,
    idle_timeout: Duration,
}

impl<T> IdleTimeoutWatchdog<T> {
    /// Construct a watchdog around `rx` with the given idle timeout.
    /// `idle_timeout` must be non-zero — zero would fire on every call
    /// before any item could arrive, which is never a useful policy.
    ///
    /// # Panics
    /// Panics if `idle_timeout.is_zero()`.
    pub fn new(rx: mpsc::Receiver<T>, idle_timeout: Duration) -> Self {
        assert!(
            !idle_timeout.is_zero(),
            "idle_timeout must be non-zero; got {idle_timeout:?}"
        );
        Self { rx, idle_timeout }
    }

    /// Wait for the next item from the receiver, bounded by the idle
    /// timeout.
    ///
    /// Returns:
    /// - `Ok(Some(item))` — producer sent an item; deadline resets on
    ///   the next call.
    /// - `Ok(None)` — the channel was closed cleanly by the producer.
    /// - `Err(IdleTimeoutElapsed)` — no item arrived within the
    ///   configured `idle_timeout`. The receiver is preserved so the
    ///   caller can choose to drop it (cancel the stream) or wait
    ///   again (retry from where we are — useful when the timeout is
    ///   advisory).
    pub async fn next(&mut self) -> Result<Option<T>, IdleTimeoutElapsed> {
        match tokio::time::timeout(self.idle_timeout, self.rx.recv()).await {
            Ok(item) => Ok(item),
            Err(_elapsed) => Err(IdleTimeoutElapsed {
                idle_timeout: self.idle_timeout,
            }),
        }
    }

    /// Consume the watchdog and return the underlying receiver.
    pub fn into_inner(self) -> mpsc::Receiver<T> {
        self.rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Producer emits items steadily within the deadline → all items
    /// flow through, no timeout fires.
    #[tokio::test(start_paused = true)]
    async fn steady_stream_completes_without_timeout() {
        let (tx, rx) = mpsc::channel::<u32>(8);
        let mut wd = IdleTimeoutWatchdog::new(rx, Duration::from_secs(5));

        tokio::spawn(async move {
            for i in 0..3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                tx.send(i).await.unwrap();
            }
            drop(tx);
        });

        assert_eq!(wd.next().await.unwrap(), Some(0));
        assert_eq!(wd.next().await.unwrap(), Some(1));
        assert_eq!(wd.next().await.unwrap(), Some(2));
        assert_eq!(wd.next().await.unwrap(), None);
    }

    /// Producer takes longer than `idle_timeout` to send anything →
    /// watchdog fires.
    #[tokio::test(start_paused = true)]
    async fn stalled_producer_trips_watchdog() {
        let (tx, rx) = mpsc::channel::<u32>(8);
        let mut wd = IdleTimeoutWatchdog::new(rx, Duration::from_millis(500));

        tokio::spawn(async move {
            // Far longer than the idle deadline — watchdog must fire
            // before we ever push.
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = tx.send(42).await;
        });

        let err = wd
            .next()
            .await
            .expect_err("watchdog must trip when producer stalls");
        assert_eq!(err.idle_timeout, Duration::from_millis(500));
    }

    /// Heartbeats reset the watchdog: a producer that emits a heartbeat
    /// just before each `idle_timeout` boundary keeps the stream alive
    /// indefinitely, even if real payload is slow.
    #[tokio::test(start_paused = true)]
    async fn heartbeats_keep_watchdog_alive_for_slow_payload() {
        #[derive(Debug, PartialEq, Eq)]
        enum Frame {
            Heartbeat,
            Payload(u32),
        }

        let (tx, rx) = mpsc::channel::<Frame>(8);
        let mut wd = IdleTimeoutWatchdog::new(rx, Duration::from_millis(500));

        tokio::spawn(async move {
            // 3 heartbeats inside the deadline, then a real payload at
            // T=2s — well past 3× the 500ms idle timeout. The stream
            // must survive purely on heartbeats.
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(400)).await;
                tx.send(Frame::Heartbeat).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
            tx.send(Frame::Payload(99)).await.unwrap();
            drop(tx);
        });

        // 3 heartbeats + 1 payload + clean close
        assert_eq!(wd.next().await.unwrap(), Some(Frame::Heartbeat));
        assert_eq!(wd.next().await.unwrap(), Some(Frame::Heartbeat));
        assert_eq!(wd.next().await.unwrap(), Some(Frame::Heartbeat));
        assert_eq!(wd.next().await.unwrap(), Some(Frame::Payload(99)));
        assert_eq!(wd.next().await.unwrap(), None);
    }

    /// Producer closes the channel cleanly with no items → `Ok(None)`,
    /// not a timeout.
    #[tokio::test(start_paused = true)]
    async fn clean_close_returns_none_not_timeout() {
        let (tx, rx) = mpsc::channel::<u32>(8);
        let mut wd = IdleTimeoutWatchdog::new(rx, Duration::from_secs(5));

        // Producer never sends; immediately drops the sender.
        drop(tx);

        assert_eq!(wd.next().await.unwrap(), None);
    }

    /// Watchdog fires almost exactly at `idle_timeout` — not earlier,
    /// not appreciably later. Uses paused virtual time so the assertion
    /// is deterministic. `tokio::time::Instant` is the paused clock;
    /// `std::time::Instant` would observe wall-clock time and produce a
    /// near-zero elapsed regardless of correctness.
    #[tokio::test(start_paused = true)]
    async fn timeout_fires_at_the_configured_deadline() {
        let (_tx, rx) = mpsc::channel::<u32>(8);
        let mut wd = IdleTimeoutWatchdog::new(rx, Duration::from_secs(3));

        let started = tokio::time::Instant::now();
        let _ = wd.next().await.expect_err("must trip");
        let elapsed = started.elapsed();

        assert!(
            elapsed >= Duration::from_secs(3),
            "watchdog fired too early: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(3_100),
            "watchdog fired too late: {elapsed:?}"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "idle_timeout must be non-zero")]
    async fn zero_timeout_panics() {
        let (_tx, rx) = mpsc::channel::<u32>(8);
        let _ = IdleTimeoutWatchdog::new(rx, Duration::ZERO);
    }
}
