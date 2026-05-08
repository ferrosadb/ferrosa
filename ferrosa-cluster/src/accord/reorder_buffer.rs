//! Reorder buffer for Accord consensus messages.
//!
//! Messages arrive in arbitrary network order but must be processed in `t0`
//! (proposal timestamp) order. The [`ReorderBuffer`] holds messages until their
//! deadline expires, then releases them sorted by `t0`.
//!
//! Deadline formula: `deadline = t0 + 2 * skew_max + rtt_p99`
//!
//! The buffer has bounded capacity and returns [`Overloaded`] when full.

use std::collections::BTreeMap;

use ferrosa_common::Timestamp;

/// Default maximum number of entries the reorder buffer will hold.
pub const DEFAULT_CAPACITY: usize = 10_000;

/// Error returned when the reorder buffer is at capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Overloaded;

impl std::fmt::Display for Overloaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reorder buffer at capacity")
    }
}

impl std::error::Error for Overloaded {}

/// An opaque message envelope stored in the reorder buffer.
///
/// In production this will wrap an Accord protocol message. For now it
/// carries the `t0` proposal timestamp and an opaque payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The proposal timestamp (`t0`) that determines processing order.
    pub t0: Timestamp,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

/// Configuration for deadline computation.
#[derive(Debug, Clone, Copy)]
pub struct TimingConfig {
    /// Maximum assumed clock skew in microseconds.
    pub skew_max_us: i64,
    /// 99th-percentile round-trip time in microseconds.
    pub rtt_p99_us: i64,
}

impl TimingConfig {
    /// Compute the release deadline for a message with the given `t0`.
    ///
    /// Formula: `deadline = t0 + 2 * skew_max + rtt_p99`
    pub fn deadline(&self, t0: Timestamp) -> Timestamp {
        t0.saturating_add(2i64.saturating_mul(self.skew_max_us))
            .saturating_add(self.rtt_p99_us)
    }
}

/// A bounded reorder buffer that delivers messages in `t0` order once their
/// deadline has passed.
pub struct ReorderBuffer {
    /// Messages keyed by t0 for ordered iteration. Multiple messages may
    /// share the same t0, so we store a Vec per key.
    entries: BTreeMap<Timestamp, Vec<Message>>,
    /// Total number of individual messages across all keys.
    len: usize,
    /// Maximum number of messages allowed.
    capacity: usize,
    /// Timing parameters for deadline computation.
    timing: TimingConfig,
}

impl ReorderBuffer {
    /// Create a new reorder buffer with the given capacity and timing config.
    pub fn new(capacity: usize, timing: TimingConfig) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        Self {
            entries: BTreeMap::new(),
            len: 0,
            capacity,
            timing,
        }
    }

    /// Insert a message into the buffer.
    ///
    /// Returns `Err(Overloaded)` if the buffer is at capacity.
    pub fn push(&mut self, msg: Message) -> Result<(), Overloaded> {
        if self.len >= self.capacity {
            return Err(Overloaded);
        }
        self.entries.entry(msg.t0).or_default().push(msg);
        self.len += 1;
        Ok(())
    }

    /// Drain all messages whose deadline has passed according to `now`.
    ///
    /// Returns messages sorted by `t0` (ascending). Messages whose
    /// `deadline(t0) <= now` are released.
    pub fn drain_ready(&mut self, now: Timestamp) -> Vec<Message> {
        let mut ready = Vec::new();
        // Collect keys whose deadline has passed.
        let cutoff_keys: Vec<Timestamp> = self
            .entries
            .keys()
            .copied()
            .take_while(|&t0| self.timing.deadline(t0) <= now)
            .collect();

        for key in cutoff_keys {
            if let Some(msgs) = self.entries.remove(&key) {
                self.len -= msgs.len();
                ready.extend(msgs);
            }
        }
        ready
    }

    /// Drain all messages regardless of deadline, in `t0` order.
    pub fn drain_all(&mut self) -> Vec<Message> {
        let mut all = Vec::with_capacity(self.len);
        for (_t0, msgs) in std::mem::take(&mut self.entries) {
            all.extend(msgs);
        }
        self.len = 0;
        all
    }

    /// Current number of messages in the buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_timing() -> TimingConfig {
        TimingConfig {
            skew_max_us: 10_000, // 10ms
            rtt_p99_us: 5_000,   // 5ms
        }
    }

    fn msg(t0: Timestamp, tag: u8) -> Message {
        Message {
            t0,
            payload: vec![tag],
        }
    }

    #[test]
    fn reorder_buffer_delivers_in_t0_order() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);

        // Insert messages in scrambled arrival order.
        buf.push(msg(300, 3)).unwrap();
        buf.push(msg(100, 1)).unwrap();
        buf.push(msg(200, 2)).unwrap();

        // Set `now` far enough in the future that all deadlines have passed.
        let now = 1_000_000;
        let ready = buf.drain_ready(now);

        assert_eq!(ready.len(), 3);
        // Must come out in t0 order: 100, 200, 300.
        assert_eq!(ready[0].t0, 100);
        assert_eq!(ready[1].t0, 200);
        assert_eq!(ready[2].t0, 300);
    }

    #[test]
    fn reorder_buffer_deadline_formula() {
        let timing = TimingConfig {
            skew_max_us: 10_000,
            rtt_p99_us: 5_000,
        };
        let t0: Timestamp = 1_000_000;
        // deadline = t0 + 2*skew_max + rtt_p99
        //          = 1_000_000 + 2*10_000 + 5_000
        //          = 1_025_000
        let expected: Timestamp = 1_025_000;
        assert_eq!(timing.deadline(t0), expected);
    }

    #[test]
    fn reorder_buffer_releases_after_deadline() {
        let timing = TimingConfig {
            skew_max_us: 10_000,
            rtt_p99_us: 5_000,
        };
        // deadline for t0=1000 => 1000 + 20000 + 5000 = 26000
        let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);

        buf.push(msg(1_000, 1)).unwrap();
        buf.push(msg(50_000, 2)).unwrap();

        // now = 26_000: exactly at deadline for t0=1000, should release it.
        // deadline for t0=50_000 = 75_000, not yet.
        let ready = buf.drain_ready(26_000);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].t0, 1_000);

        // Second message still held.
        assert_eq!(buf.len(), 1);

        // Advance past second deadline.
        let ready2 = buf.drain_ready(75_000);
        assert_eq!(ready2.len(), 1);
        assert_eq!(ready2[0].t0, 50_000);
        assert!(buf.is_empty());
    }

    #[test]
    fn reorder_buffer_overflow_backpressure() {
        let timing = default_timing();
        let capacity = 3;
        let mut buf = ReorderBuffer::new(capacity, timing);

        buf.push(msg(1, 1)).unwrap();
        buf.push(msg(2, 2)).unwrap();
        buf.push(msg(3, 3)).unwrap();

        // Buffer is full — next push must fail.
        let result = buf.push(msg(4, 4));
        assert_eq!(result, Err(Overloaded));
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn reorder_buffer_empty_after_drain() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);

        buf.push(msg(100, 1)).unwrap();
        buf.push(msg(200, 2)).unwrap();
        buf.push(msg(300, 3)).unwrap();

        let all = buf.drain_all();
        assert_eq!(all.len(), 3);
        // Drained in t0 order.
        assert_eq!(all[0].t0, 100);
        assert_eq!(all[1].t0, 200);
        assert_eq!(all[2].t0, 300);

        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);

        // drain_all on empty buffer returns nothing.
        let empty = buf.drain_all();
        assert!(empty.is_empty());
    }

    #[test]
    fn reorder_buffer_preserves_arrival_order_for_equal_timestamps() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);

        buf.push(msg(100, 1)).unwrap();
        buf.push(msg(100, 2)).unwrap();
        buf.push(msg(100, 3)).unwrap();

        let drained = buf.drain_all();
        let payloads: Vec<u8> = drained.iter().map(|m| m.payload[0]).collect();
        assert_eq!(payloads, vec![1, 2, 3]);
    }

    #[test]
    fn reorder_buffer_orders_all_three_message_arrival_permutations_deterministically() {
        let timing = default_timing();
        let permutations = [
            [(100, 1), (200, 2), (300, 3)],
            [(100, 1), (300, 3), (200, 2)],
            [(200, 2), (100, 1), (300, 3)],
            [(200, 2), (300, 3), (100, 1)],
            [(300, 3), (100, 1), (200, 2)],
            [(300, 3), (200, 2), (100, 1)],
        ];

        for permutation in permutations {
            let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);
            for (t0, tag) in permutation {
                buf.push(msg(t0, tag)).unwrap();
            }

            let payloads: Vec<u8> = buf.drain_all().iter().map(|m| m.payload[0]).collect();
            assert_eq!(payloads, vec![1, 2, 3], "permutation {permutation:?}");
        }
    }

    #[test]
    fn reorder_buffer_drains_medium_deterministic_batch_under_tight_bound() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);
        for i in (0..1_000i64).rev() {
            buf.push(msg(i, (i % 251) as u8)).unwrap();
        }

        let started = std::time::Instant::now();
        let drained = buf.drain_all();
        let elapsed = started.elapsed();

        assert_eq!(drained.len(), 1_000);
        assert!(drained.windows(2).all(|w| w[0].t0 <= w[1].t0));
        assert!(
            elapsed < std::time::Duration::from_millis(10),
            "draining 1000 deterministic Accord messages took {elapsed:?}"
        );
    }
}
