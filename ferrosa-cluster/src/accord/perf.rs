//! Performance baseline micro-benchmarks for Accord state machine overhead.
//!
//! These tests use `Instant::now()` deltas (not criterion) to measure latency
//! of core Accord paths. They assert completion within reasonable bounds for
//! the unit-test code path (< 1ms).
//!
//! # Tests (A5.6)
//!
//! - `perf_single_key_write_p50` — P50 write latency through AccordStateMachine
//! - `perf_single_key_write_p99` — P99 write latency
//! - `perf_single_key_read_p50` — read latency through linearizable path

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::linearizable_read::LinearizableReadManager;
    use crate::accord::state_machine::AccordStateMachine;
    use ferrosa_storage::accord::conflict_index::{ConflictIndex, InFlightWrite, TxnStatus};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
    }

    fn make_sm(node_id: u64) -> (AccordStateMachine, Arc<MockSyncWriter>) {
        let writer = Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::new(node_id, writer.clone());
        (sm, writer)
    }

    /// Compute the P-th percentile from a sorted slice of durations (in nanos).
    fn percentile(sorted_nanos: &[u64], p: f64) -> u64 {
        assert!(!sorted_nanos.is_empty(), "need at least one sample");
        assert!((0.0..=100.0).contains(&p), "percentile must be 0..=100");
        let idx = ((p / 100.0) * (sorted_nanos.len() as f64 - 1.0)).ceil() as usize;
        sorted_nanos[idx.min(sorted_nanos.len() - 1)]
    }

    /// Run a single-key write (PreAccept -> Accept -> Commit -> Apply)
    /// through the state machine and return the elapsed time in nanoseconds.
    fn single_key_write_nanos(sm: &mut AccordStateMachine, base_time: u64) -> u64 {
        let txn_id = txn(1, base_time);
        let t0 = ts(base_time);
        let key = b"perf_bench_key";

        let start = Instant::now();

        // Full write path: PreAccept -> Accept -> Commit -> Apply
        sm.handle_preaccept(txn_id, t0, key, BallotNumber(0), 0);
        sm.handle_accept(txn_id, t0, ts(base_time + 1), vec![], BallotNumber(1));
        sm.handle_commit(txn_id, t0, ts(base_time + 1), vec![]);
        sm.handle_apply(txn_id, vec![42]);

        start.elapsed().as_nanos() as u64
    }

    // -----------------------------------------------------------------------
    // Test 1: perf_single_key_write_p50
    // -----------------------------------------------------------------------

    /// P50 latency for a single-key write through the full Accord state
    /// machine path (PreAccept -> Accept -> Commit -> Apply).
    #[test]
    fn perf_single_key_write_p50() {
        let (mut sm, _writer) = make_sm(1);
        let iterations = 200;

        // Warm-up: 10 iterations to stabilize allocator / JIT-like effects.
        for i in 0..10u64 {
            single_key_write_nanos(&mut sm, 1_000_000 + i * 100);
        }

        // Measured iterations.
        let mut samples: Vec<u64> = (0..iterations)
            .map(|i| single_key_write_nanos(&mut sm, 2_000_000 + i * 100))
            .collect();

        samples.sort_unstable();
        let p50 = percentile(&samples, 50.0);

        // Assert P50 < 1ms (1_000_000 ns) — should be well under for unit test.
        assert!(
            p50 < 1_000_000,
            "P50 write latency {p50}ns exceeds 1ms budget"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: perf_single_key_write_p99
    // -----------------------------------------------------------------------

    /// P99 latency for single-key writes. Tail latency should still be < 1ms
    /// in the unit test code path (no real I/O).
    #[test]
    fn perf_single_key_write_p99() {
        let (mut sm, _writer) = make_sm(1);
        let iterations = 200;

        // Warm-up.
        for i in 0..10u64 {
            single_key_write_nanos(&mut sm, 3_000_000 + i * 100);
        }

        let mut samples: Vec<u64> = (0..iterations)
            .map(|i| single_key_write_nanos(&mut sm, 4_000_000 + i * 100))
            .collect();

        samples.sort_unstable();
        let p99 = percentile(&samples, 99.0);

        assert!(
            p99 < 1_000_000,
            "P99 write latency {p99}ns exceeds 1ms budget"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: perf_single_key_read_p50
    // -----------------------------------------------------------------------

    /// P50 latency for a linearizable read check (conflict index lookup).
    /// This measures the read-side overhead of the Accord protocol.
    #[test]
    fn perf_single_key_read_p50() {
        let mgr = LinearizableReadManager::new();
        let mut conflict_index = ConflictIndex::new(100_000);
        let key = b"perf_read_key";

        // Populate the conflict index with some in-flight writes to
        // simulate a realistic environment.
        for i in 0..50u64 {
            let write = InFlightWrite {
                txn_id: txn(1, 5_000_000 + i * 100),
                t0: ts(5_000_000 + i * 100),
                accord_ts: None,
                status: TxnStatus::PreAccepted,
            };
            conflict_index.register(key, write).unwrap();
        }

        let iterations = 200;

        // Warm-up.
        for _ in 0..10 {
            mgr.check_conflicts(&conflict_index, key);
        }

        // Measured iterations.
        let mut samples: Vec<u64> = (0..iterations)
            .map(|_| {
                let start = Instant::now();
                let _result = mgr.check_conflicts(&conflict_index, key);
                start.elapsed().as_nanos() as u64
            })
            .collect();

        samples.sort_unstable();
        let p50 = percentile(&samples, 50.0);

        assert!(
            p50 < 1_000_000,
            "P50 read latency {p50}ns exceeds 1ms budget"
        );
    }
}
