//! Performance baseline micro-benchmarks for Accord state machine overhead.
//!
//! These benchmarks measure the MINIMUM cost of one operation, as a multiple of
//! a same-run CPU reference loop — never an absolute nanosecond bound. An
//! absolute `p50 < 1 ms` measures the host: on a contended machine the
//! scheduler steals the measured thread and the "latency" is the deschedule
//! gap, not the code (see `perf_support` and `perf_regression`).
//!
//! The MINIMUM op cost is load-independent: it is the cost of one op that ran
//! without being descheduled. On a shared 18-core box from load 0 to load 150
//! it did not move (measured baselines below), while the absolute p99 moved by
//! more than an order of magnitude. A uniform regression (a clone added to the
//! hot path) raises the minimum; a pure TAIL regression does not, and that is
//! measured on dedicated hardware (the Fly perf-tier rigs), not here.
//!
//! The names keep their historical `p50`/`p99` suffixes for continuity, but the
//! bound each asserts is the load-independent min-op cost. The deterministic
//! half (every call returns, the path completes) is asserted alongside.
//!
//! # Tests (A5.6)
//!
//! - `perf_single_key_write_p50` — single-key write path cost through the state
//!   machine (PreAccept -> Accept -> Commit -> Apply). Baseline ~0.039 loops.
//! - `perf_single_key_write_p99` — the same path over more samples, guarding
//!   against a uniform slowdown. Baseline ~0.039 loops.
//! - `perf_single_key_read_p50` — linearizable-read conflict check. Baseline
//!   ~0.13 loops.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};
    use ferrosa_storage::accord::conflict_index::{ConflictIndex, InFlightWrite, TxnStatus};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::linearizable_read::LinearizableReadManager;
    use crate::accord::perf_support::assert_min_op_in_reference_loops;
    use crate::accord::state_machine::AccordStateMachine;

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

    /// Run a single-key write (PreAccept -> Accept -> Commit -> Apply) through
    /// the state machine and return the elapsed time in nanoseconds.
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

    /// The minimum single-key-write cost over `iterations` samples, as the
    /// benchmark measures it. Warm-up runs first so allocator effects settle.
    fn min_single_key_write_ns(iterations: u64, warmup: u64, base: u64) -> u128 {
        let (mut sm, _writer) = make_sm(1);
        for i in 0..warmup {
            single_key_write_nanos(&mut sm, base + i * 100);
        }
        let mut min = u128::MAX;
        for i in 0..iterations {
            let dt = single_key_write_nanos(&mut sm, base + (warmup + i) * 100) as u128;
            min = min.min(dt);
        }
        min
    }

    // -----------------------------------------------------------------------
    // Test 1: perf_single_key_write_p50
    // -----------------------------------------------------------------------

    /// Minimum cost of one single-key write through the full Accord state
    /// machine path (PreAccept -> Accept -> Commit -> Apply).
    #[test]
    fn perf_single_key_write_p50() {
        // Budget: 0.5 reference loops (baseline ~0.039). A clone added to the
        // write path is a multiple of this.
        let _ = assert_min_op_in_reference_loops("single-key write", 0.5, 5, || {
            min_single_key_write_ns(200, 10, 2_000_000)
        });
    }

    // -----------------------------------------------------------------------
    // Test 2: perf_single_key_write_p99
    // -----------------------------------------------------------------------

    /// Minimum cost of one single-key write over more samples. This is the
    /// load-independent form of the old `p99 < 5 ms` bound: the p99 itself is
    /// only measurable on a quiet machine, so the guard is on the cheapest op,
    /// which a uniform regression still raises.
    #[test]
    fn perf_single_key_write_p99() {
        // Budget: 0.5 reference loops (baseline ~0.039).
        let _ = assert_min_op_in_reference_loops("single-key write (tail set)", 0.5, 5, || {
            min_single_key_write_ns(400, 10, 4_000_000)
        });
    }

    // -----------------------------------------------------------------------
    // Test 3: perf_single_key_read_p50
    // -----------------------------------------------------------------------

    /// Minimum cost of a linearizable read check (conflict index lookup).
    /// This measures the read-side overhead of the Accord protocol.
    #[test]
    fn perf_single_key_read_p50() {
        // Budget: 1.0 reference loops (baseline ~0.13).
        let _ = assert_min_op_in_reference_loops("single-key read check", 1.0, 5, || {
            let mgr = LinearizableReadManager::new();
            let mut conflict_index = ConflictIndex::new(100_000);
            let key = b"perf_read_key";

            // Populate the conflict index with in-flight writes, as the
            // benchmark does.
            for i in 0..50u64 {
                let write = InFlightWrite {
                    txn_id: txn(1, 5_000_000 + i * 100),
                    t0: ts(5_000_000 + i * 100),
                    accord_ts: None,
                    status: TxnStatus::PreAccepted,
                };
                conflict_index.register(key, write).unwrap();
            }

            // Warm-up.
            for _ in 0..10 {
                mgr.check_conflicts(&conflict_index, key);
            }

            let mut min = u128::MAX;
            for _ in 0..200 {
                let start = Instant::now();
                let _result = mgr.check_conflicts(&conflict_index, key);
                min = min.min(start.elapsed().as_nanos());
            }
            min
        });
    }
}
