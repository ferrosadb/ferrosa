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
    ///
    /// `deadline(t0)` is monotone in `t0` (the formula adds a constant), so
    /// the ready prefix is contiguous. We locate the first non-ready key
    /// and `split_off` once — O(log N + R) instead of O(N log N) per-key
    /// removes, which matters when all entries are eligible (the hot path)
    /// and the per-key remove fan-out otherwise dominates a benchmark.
    pub fn drain_ready(&mut self, now: Timestamp) -> Vec<Message> {
        // Locate the first key whose deadline has NOT yet passed. Everything
        // strictly less than this key is releasable.
        let first_not_ready = self
            .entries
            .keys()
            .copied()
            .find(|&t0| self.timing.deadline(t0) > now);

        let ready_entries = match first_not_ready {
            // All keys are ready — take the whole map in one move.
            None => std::mem::take(&mut self.entries),
            // Split: `self.entries` keeps keys < cut (ready), `kept` holds
            // keys >= cut (still pending). Swap so `self.entries` becomes
            // the pending half.
            Some(cut) => {
                let kept = self.entries.split_off(&cut);
                std::mem::replace(&mut self.entries, kept)
            }
        };

        // Flatten in t0 order. BTreeMap iterates ascending, so the resulting
        // vector is sorted by t0; per-key arrival order is preserved within
        // each bucket.
        let mut ready = Vec::with_capacity(ready_entries.values().map(Vec::len).sum());
        for (_t0, msgs) in ready_entries {
            ready.extend(msgs);
        }
        self.len -= ready.len();
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

    /// Draining a medium batch pushed in reverse arrival order returns every
    /// message, ordered by `t0`.
    ///
    /// This deliberately asserts no wall-clock bound. It used to also require
    /// the drain to finish in under 10 ms, which measures the RUNNER rather
    /// than the code: a contended merge-group build clocked it at 10.2 ms and
    /// ejected PR #343 from the merge queue. That is the same failure
    /// `accord::perf_regression` was created to contain — see its module docs,
    /// which record an earlier 52.8 ms ejection of a docs-only PR
    /// (forge t_430e21f7) — and `ci.yml` skips that module in the per-PR lane
    /// for exactly this reason.
    ///
    /// The timing guard is not lost: `perf_regression` measures the identical
    /// drain against a same-run CPU reference loop on the nightly
    /// `perf-regression` job, and the deterministic guards (one output
    /// allocation, linear per-message cost) in
    /// `tests/reorder_buffer_drain_budget.rs` run in the default suite. This
    /// test keeps the other deterministic half — completeness and ordering —
    /// which is what belongs in the merge lane.
    #[test]
    fn reorder_buffer_drains_medium_batch_completely_and_in_t0_order() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(DEFAULT_CAPACITY, timing);
        for i in (0..1_000i64).rev() {
            buf.push(msg(i, (i % 251) as u8)).unwrap();
        }

        let drained = buf.drain_all();

        assert_eq!(drained.len(), 1_000);
        assert!(drained.windows(2).all(|w| w[0].t0 <= w[1].t0));
    }

    // -----------------------------------------------------------------------
    // Invariant tests. The ReorderBuffer sits on the consensus message path,
    // so dropping, duplicating or reordering a message is a CORRECTNESS
    // failure, not a perf one. Each invariant below is pinned independently,
    // and the differential test is the oracle for any future change to the
    // internal structure.
    // -----------------------------------------------------------------------

    /// A deliberately simple reference model of the buffer's observable
    /// behaviour: a flat arrival-ordered list. It is O(n log n) and obviously
    /// correct, which is the point — it is the ORACLE the ordered
    /// `BTreeMap` implementation is differentially tested against. A change
    /// to the internal structure that alters what `drain_ready` returns
    /// (ordering, completeness, deadline gating) fails against it.
    struct ReferenceModel {
        /// `(t0, arrival_seq, message)` in arrival order.
        entries: Vec<(Timestamp, u64, Message)>,
        next_seq: u64,
    }

    impl ReferenceModel {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                next_seq: 0,
            }
        }

        fn push(&mut self, msg: Message) {
            let seq = self.next_seq;
            self.next_seq += 1;
            self.entries.push((msg.t0, seq, msg));
        }

        fn len(&self) -> usize {
            self.entries.len()
        }

        /// Everything whose deadline has passed, ordered by `(t0, arrival_seq)`.
        fn drain_ready(&mut self, now: Timestamp, timing: &TimingConfig) -> Vec<Message> {
            let all = std::mem::take(&mut self.entries);
            let (mut ready, kept): (Vec<_>, Vec<_>) = all
                .into_iter()
                .partition(|(t0, _, _)| timing.deadline(*t0) <= now);
            self.entries = kept;
            ready.sort_by_key(|(t0, seq, _)| (*t0, *seq));
            ready.into_iter().map(|(_, _, m)| m).collect()
        }

        /// Everything, ordered by `(t0, arrival_seq)`.
        fn drain_all(&mut self) -> Vec<Message> {
            let mut all = std::mem::take(&mut self.entries);
            all.sort_by_key(|(t0, seq, _)| (*t0, *seq));
            all.into_iter().map(|(_, _, m)| m).collect()
        }
    }

    /// Deterministic xorshift so the differential test is reproducible.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    /// Order-independent fingerprint of a message set, for multiset equality.
    fn multiset(mut msgs: Vec<Message>) -> Vec<(Timestamp, Vec<u8>)> {
        msgs.sort_by(|a, b| (a.t0, &a.payload).cmp(&(b.t0, &b.payload)));
        msgs.into_iter().map(|m| (m.t0, m.payload)).collect()
    }

    /// INVARIANT (a) COMPLETENESS — every pushed message is returned exactly
    /// once across interleaved drains: nothing lost, nothing duplicated.
    #[test]
    fn reorder_buffer_returns_every_message_exactly_once_across_interleaved_drains() {
        let timing = default_timing(); // deadline = t0 + 25_000
        let mut buf = ReorderBuffer::new(4_000, timing);

        // Distinct t0 buckets *and* duplicate t0 values, with payloads drawn
        // from a small range so a dropped/duplicated message is detectable.
        let pushed: Vec<Message> = (0..3_000i64)
            .map(|i| msg((i % 17) * 1_000, (i % 251) as u8))
            .collect();
        for m in &pushed {
            buf.push(m.clone()).unwrap();
        }

        let mut drained: Vec<Message> = Vec::new();
        for step in 0..20i64 {
            // t0 in {0,1000..16000} => deadline in {25000..41000}; this walks
            // the ready prefix across the whole set.
            let now = step * 1_500 + 25_000;
            drained.extend(buf.drain_ready(now));
        }
        drained.extend(buf.drain_all());

        assert_eq!(drained.len(), 3_000, "every message must come back");
        assert_eq!(
            multiset(drained),
            multiset(pushed),
            "the drained multiset must equal the pushed multiset exactly"
        );
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    /// INVARIANT (b) ORDERING — arrival order is preserved within equal t0,
    /// even when those messages are interleaved with a different key's
    /// arrivals (stronger than the same-key-only test above).
    #[test]
    fn reorder_buffer_preserves_arrival_order_within_equal_t0_across_interleaved_keys() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(100, timing);

        buf.push(msg(100, 1)).unwrap();
        buf.push(msg(50, 9)).unwrap();
        buf.push(msg(100, 2)).unwrap();
        buf.push(msg(100, 3)).unwrap();
        buf.push(msg(50, 8)).unwrap();
        buf.push(msg(100, 4)).unwrap();

        let out = buf.drain_all();
        assert!(out.windows(2).all(|w| w[0].t0 <= w[1].t0), "t0 ascending");
        let at_100: Vec<u8> = out
            .iter()
            .filter(|m| m.t0 == 100)
            .map(|m| m.payload[0])
            .collect();
        let at_50: Vec<u8> = out
            .iter()
            .filter(|m| m.t0 == 50)
            .map(|m| m.payload[0])
            .collect();
        assert_eq!(at_100, vec![1, 2, 3, 4], "arrival order within t0=100");
        assert_eq!(at_50, vec![9, 8], "arrival order within t0=50");
    }

    /// INVARIANT (c) PREFIX GATING — `drain_ready(now)` releases exactly the
    /// contiguous ready prefix and no message whose deadline has not passed.
    #[test]
    fn reorder_buffer_drain_ready_releases_exactly_the_contiguous_ready_prefix() {
        // deadline = t0 + 2*10 + 5 = t0 + 25.
        let timing = TimingConfig {
            skew_max_us: 10,
            rtt_p99_us: 5,
        };
        let mut buf = ReorderBuffer::new(100, timing);
        for k in [0i64, 10, 20, 30, 40] {
            buf.push(msg(k, k as u8)).unwrap();
        }

        // now = 45: deadline(20) = 45 is ready (<= now); deadline(30) = 55 is not.
        let ready = buf.drain_ready(45);
        assert_eq!(
            ready.iter().map(|m| m.t0).collect::<Vec<_>>(),
            vec![0, 10, 20],
            "only the ready prefix is released"
        );
        assert_eq!(buf.len(), 2);

        // The not-ready suffix is untouched and still ordered.
        let rest = buf.drain_all();
        assert_eq!(rest.iter().map(|m| m.t0).collect::<Vec<_>>(), vec![30, 40]);

        // And nothing is released before its deadline.
        let mut buf2 = ReorderBuffer::new(100, timing);
        buf2.push(msg(100, 1)).unwrap(); // deadline 125
        assert!(buf2.drain_ready(124).is_empty(), "must not release early");
        assert_eq!(buf2.len(), 1);
    }

    /// INVARIANT (d) CAPACITY + LEN ACCOUNTING — the `Overloaded` contract and
    /// exact `len` accounting are unchanged, including after a drain and after
    /// a refused push.
    #[test]
    fn reorder_buffer_len_and_capacity_are_accounted_exactly() {
        let timing = default_timing();
        let mut buf = ReorderBuffer::new(3, timing);

        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());

        buf.push(msg(100, 1)).unwrap();
        assert_eq!(buf.len(), 1);
        assert!(!buf.is_empty());

        // A duplicate t0 shares a bucket but still counts one message each.
        buf.push(msg(100, 2)).unwrap();
        assert_eq!(buf.len(), 2);
        buf.push(msg(200, 3)).unwrap();
        assert_eq!(buf.len(), 3);

        // At capacity: refused, and `len` must not move.
        assert_eq!(buf.push(msg(300, 4)), Err(Overloaded));
        assert_eq!(buf.len(), 3);

        // Draining a ready prefix decrements by exactly the drained count.
        let ready = buf.drain_ready(1_000_000); // every deadline has passed
        assert_eq!(ready.len(), 3);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());

        // A refused push left no phantom entry behind: capacity is available.
        buf.push(msg(400, 5)).unwrap();
        assert_eq!(buf.len(), 1);
    }

    /// INVARIANT (e) DIFFERENTIAL — over randomized push/drain sequences the
    /// implementation is output-identical to the flat reference model, for
    /// both `drain_ready` and `drain_all`, including `len` and the
    /// `Overloaded` boundary. This is the guard that makes an internal
    /// restructuring safe: any structural optimization must move no observable
    /// row relative to the model.
    #[test]
    fn reorder_buffer_matches_reference_model_over_randomized_push_drain_sequences() {
        // deadline = t0 + 25, so `now` drawn from 0..=70 straddles the ready
        // prefix boundary for t0 drawn from 0..40.
        let timing = TimingConfig {
            skew_max_us: 10,
            rtt_p99_us: 5,
        };
        let capacity = 120usize;
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut buf = ReorderBuffer::new(capacity, timing);
        let mut model = ReferenceModel::new();
        let mut ever_drained: Vec<Message> = Vec::new();

        for step in 0..2_000u64 {
            if rng.next_u64().is_multiple_of(2) {
                let t0 = (rng.next_u64() % 40) as Timestamp;
                let tag = (rng.next_u64() % 4) as u8;
                let m = msg(t0, tag);
                let before = model.len();
                let result = buf.push(m.clone());
                if before < capacity {
                    assert!(
                        result.is_ok(),
                        "step {step}: push under capacity must succeed"
                    );
                    model.push(m);
                } else {
                    assert_eq!(
                        result,
                        Err(Overloaded),
                        "step {step}: push at capacity must be refused"
                    );
                }
            } else {
                let now = (rng.next_u64() % 71) as Timestamp;
                let got = buf.drain_ready(now);
                let want = model.drain_ready(now, &timing);
                assert_eq!(
                    multiset(got.clone()),
                    multiset(want),
                    "step {step}: drain_ready({now}) diverged from the reference model"
                );
                ever_drained.extend(got);
            }
            assert_eq!(
                buf.len(),
                model.len(),
                "step {step}: len accounting diverged from the reference model"
            );
        }

        // Final flush must agree too, and the whole run must be complete.
        let got = buf.drain_all();
        assert_eq!(multiset(got.clone()), multiset(model.drain_all()));
        ever_drained.extend(got);
        assert!(buf.is_empty());
        assert_eq!(
            ever_drained.len(),
            model.next_seq as usize,
            "every pushed message must be returned exactly once"
        );
    }
}
