//! Performance regression test suite for Accord.
//!
//! Performance tests that verify operation latency stays within acceptable
//! thresholds. The ReorderBuffer drain bound is measured RELATIVE to a same-run
//! CPU reference loop rather than against an absolute wall-clock figure (`< 10
//! ms`), because an absolute bound measures the runner.
//!
//! # Where these run
//!
//! NOT in the per-PR / merge-queue lane. `ci.yml` skips `accord::perf_regression`
//! and the suite runs nightly (`nightly-fuzz.yml`, job `perf-regression`).
//!
//! # Why the drain bound is not absolute
//!
//! The module originally asserted an absolute `< 10 ms` bound on a 1000-message
//! drain. That bound ejected a docs-only PR from the merge queue when a
//! contended merge-group build clocked the drain at 52.8 ms (forge t_430e21f7),
//! and on 2026-09-30 the nightly fuzz lane ejected it again at 56.7 ms while
//! the dedicated `perf-regression` job PASSED in the very same workflow — same
//! commit, same code, only runner load differing. Measured on an idle machine
//! the drain takes ~0.16 ms and allocates once; the 56.7 ms was the runner.
//!
//! The bound is now `DRAIN_BUDGET_IN_REFERENCE_LOOPS` reference loops, not a
//! number of milliseconds: both quantities are CPU-bound and are slowed by
//! contention together, so the ratio is stable while the absolute figure is
//! not. The threshold was NOT raised — it was made load-independent. The
//! deterministic half (allocation count per message, linear cost) runs in the
//! default suite as `tests/reorder_buffer_drain_budget.rs` and is what guards
//! the per-PR lane; the remaining `accord::perf_regression` bounds are still
//! absolute and still catch GROSS regressions only.
//!
//! # A7.9 Tests
//!
//! - `perf_regression_suite` — all benchmarks pass thresholds
//! - `perf_multi_key_txn_p50` — multi-key transaction latency
//! - `perf_conflict_index_lookup_p99` — conflict index lookup latency
//! - `perf_reorder_buffer_overhead_p99` — reorder buffer overhead

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use ferrosa_common::accord::{BallotNumber, Timestamp as AccordTimestamp, TxnId};
    use ferrosa_storage::accord::conflict_index::{ConflictIndex, InFlightWrite, TxnStatus};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::reorder_buffer::{Message, ReorderBuffer, TimingConfig};
    use crate::accord::state_machine::AccordStateMachine;
    use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn ts(micros: u64) -> AccordTimestamp {
        AccordTimestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
    }

    /// Compute the p-th percentile from a sorted slice of durations (in nanos).
    fn percentile(sorted_nanos: &[u128], p: f64) -> u128 {
        assert!(!sorted_nanos.is_empty(), "need at least one sample");
        assert!((0.0..=100.0).contains(&p), "percentile must be 0..100");
        let idx = ((p / 100.0) * (sorted_nanos.len() - 1) as f64).round() as usize;
        sorted_nanos[idx.min(sorted_nanos.len() - 1)]
    }

    // -----------------------------------------------------------------------
    // Machine calibration
    // -----------------------------------------------------------------------

    /// A fixed, CPU-bound arithmetic loop whose cost depends only on how fast
    /// this machine is executing scalar code right now.
    fn reference_loop_ns() -> u128 {
        let work = || {
            let mut acc: u64 = 0;
            for i in 0..40_000u64 {
                acc = acc
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(i ^ acc);
            }
            acc
        };
        // Take the MINIMUM: the least-descheduled sample is the one that
        // reflects the machine's actual throughput rather than the scheduler's.
        (0..5)
            .map(|_| {
                let start = Instant::now();
                std::hint::black_box(work());
                start.elapsed().as_nanos()
            })
            .min()
            .expect("at least one sample")
    }

    /// How many reference loops a 1000-message all-ready drain may cost.
    ///
    /// This replaces the absolute `< 10 ms` bound, which measured the runner:
    /// a contended shared runner clocked the same drain at 56.7 ms while an
    /// idle box measures ~0.16 ms (see the module docs and forge t_430e21f7).
    /// Both quantities here are CPU-bound and are slowed by contention
    /// together, so their RATIO is stable across load (measured 0.74 idle vs
    /// 0.76 under 64 spinning threads on an 18-core box). 6 leaves ~8x
    /// headroom over that baseline while still failing on a real algorithmic
    /// regression, which multiplies the drain cost but not the reference loop.
    ///
    /// The threshold was NOT raised — it was made load-independent. The
    /// load-independent behavioural guards live in
    /// `tests/reorder_buffer_drain_budget.rs` (allocation count per message and
    /// linear cost), which run in the default suite and police the per-PR lane.
    const DRAIN_BUDGET_IN_REFERENCE_LOOPS: f64 = 6.0;

    // =======================================================================
    // A7.9-T1: perf_regression_suite
    // =======================================================================

    /// All benchmarks pass thresholds:
    /// - Single-key PreAccept < 1ms
    /// - Commit < 1ms
    /// - ConflictIndex lookup < 100us
    /// - ReorderBuffer push p99 < 100us
    /// - ReorderBuffer drain (1000 msgs) < `DRAIN_BUDGET_IN_REFERENCE_LOOPS`
    ///   reference loops, NOT < 10 ms absolute (see the module docs)
    #[test]
    fn perf_regression_suite() {
        // --- Single-key PreAccept benchmark ---
        let writer = Arc::new(MockSyncWriter::new());
        let mut sm = AccordStateMachine::new(1, writer);

        let mut preaccept_durations = Vec::new();
        for i in 0..100u64 {
            let tid = txn(1, 1000 + i);
            let t0 = ts(1000 + i);
            let start = Instant::now();
            let _resp = sm.handle_preaccept(tid, t0, b"key", BallotNumber(0), 0);
            preaccept_durations.push(start.elapsed().as_nanos());
        }

        preaccept_durations.sort();
        let p50 = percentile(&preaccept_durations, 50.0);
        assert!(
            p50 < 1_000_000, // 1ms
            "PreAccept p50 = {}ns exceeds 1ms threshold",
            p50
        );

        // --- Commit benchmark ---
        let writer2 = Arc::new(MockSyncWriter::new());
        let mut sm2 = AccordStateMachine::new(2, writer2);
        let mut commit_durations = Vec::new();

        for i in 0..100u64 {
            let tid = txn(2, 2000 + i);
            let t0 = ts(2000 + i);
            sm2.handle_preaccept(tid, t0, b"ckey", BallotNumber(0), 0);
            sm2.handle_accept(tid, t0, ts(2001 + i), vec![], BallotNumber(1));

            let start = Instant::now();
            sm2.handle_commit(tid, t0, ts(2001 + i), vec![]);
            commit_durations.push(start.elapsed().as_nanos());
        }

        commit_durations.sort();
        let p50_commit = percentile(&commit_durations, 50.0);
        assert!(
            p50_commit < 1_000_000,
            "Commit p50 = {}ns exceeds 1ms threshold",
            p50_commit
        );

        // --- ConflictIndex lookup benchmark ---
        let mut idx = ConflictIndex::new(100_000);
        for i in 0..1000u64 {
            let entry = InFlightWrite {
                txn_id: TxnId(ts(i)),
                t0: ts(i),
                accord_ts: None,
                status: TxnStatus::PreAccepted,
            };
            idx.register(format!("k:{}", i).as_bytes(), entry).unwrap();
        }

        let mut lookup_durations = Vec::new();
        for i in 0..1000u64 {
            let key = format!("k:{}", i);
            let start = Instant::now();
            let _ = idx.max_conflicting_timestamp(key.as_bytes());
            lookup_durations.push(start.elapsed().as_nanos());
        }

        lookup_durations.sort();
        let p99_lookup = percentile(&lookup_durations, 99.0);
        assert!(
            p99_lookup < 100_000, // 100us
            "ConflictIndex lookup p99 = {}ns exceeds 100us threshold",
            p99_lookup
        );

        // --- ReorderBuffer push+drain benchmark ---
        let timing = TimingConfig {
            skew_max_us: 10_000,
            rtt_p99_us: 5_000,
        };

        let mut rb_durations = Vec::new();
        // Best-of-5 all-ready drain of 1000 distinct-t0 messages. `min` is the
        // sample least perturbed by contention; the reference loop is measured
        // the same way, so contention largely cancels in the ratio below.
        let mut drain_ns = u128::MAX;
        for _ in 0..5 {
            let mut buf = ReorderBuffer::new(10_000, timing);
            for i in 0..1000i64 {
                let msg = Message {
                    t0: i * 100,
                    payload: vec![(i & 0xFF) as u8],
                };
                let start = Instant::now();
                buf.push(msg).unwrap();
                rb_durations.push(start.elapsed().as_nanos());
            }

            let start = Instant::now();
            let _ = buf.drain_ready(i64::MAX);
            drain_ns = drain_ns.min(start.elapsed().as_nanos());
        }

        rb_durations.sort();
        let p99_rb = percentile(&rb_durations, 99.0);
        assert!(
            p99_rb < 100_000,
            "ReorderBuffer push p99 = {}ns exceeds 100us threshold",
            p99_rb
        );

        // Load-independent drain guard: compare against a same-run reference
        // loop instead of an absolute clock. `DRAIN_BUDGET_IN_REFERENCE_LOOPS`
        // is the load-independent form of the original `< 10 ms`; the raw
        // nanosecond figure is still reported in the failure message.
        let reference_ns = reference_loop_ns();
        let budget_ns = DRAIN_BUDGET_IN_REFERENCE_LOOPS * reference_ns as f64;
        let ratio = drain_ns as f64 / reference_ns as f64;
        assert!(
            (drain_ns as f64) < budget_ns,
            "ReorderBuffer drain 1000 msgs = {}ns = {:.2}x the reference loop; \
             budget is {:.2}x ({}ns on this machine). The absolute 10ms bound \
             this replaced measured the runner, not the code",
            drain_ns,
            ratio,
            DRAIN_BUDGET_IN_REFERENCE_LOOPS,
            budget_ns as u128
        );
    }

    // =======================================================================
    // A7.9-T2: perf_multi_key_txn_p50
    // =======================================================================

    /// Multi-key transaction latency: a 3-shard transaction through TestCluster
    /// must complete within latency threshold.
    #[test]
    fn perf_multi_key_txn_p50() {
        let mut durations = Vec::new();

        for trial in 0..50u64 {
            let mut cluster = TestCluster::new(3);
            let replicas = vec![1, 2, 3];
            let t0_micros = 10000 + trial * 1000;
            let t0 = ts(t0_micros);
            let tid = txn(1, t0_micros);

            let start = Instant::now();

            // PreAccept to all replicas for 3 different keys.
            for (key_idx, &r) in replicas.iter().enumerate() {
                cluster.send(TestMessage {
                    src: 1,
                    dst: r,
                    payload: TestMessagePayload::PreAccept {
                        txn_id: tid,
                        t0,
                        key: format!("multi:key:{}", key_idx).into_bytes(),
                    },
                });
            }
            cluster.drain();

            // Commit.
            for &r in &replicas {
                cluster.send(TestMessage {
                    src: 1,
                    dst: r,
                    payload: TestMessagePayload::Commit {
                        txn_id: tid,
                        t0,
                        t: t0,
                        deps: vec![],
                    },
                });
            }
            cluster.drain();

            durations.push(start.elapsed().as_nanos());
        }

        durations.sort();
        let p50 = percentile(&durations, 50.0);
        // Multi-key deterministic txn should complete well under 10ms.
        assert!(
            p50 < 10_000_000,
            "multi-key txn p50 = {}ns exceeds 10ms threshold",
            p50
        );
    }

    // =======================================================================
    // A7.9-T3: perf_conflict_index_lookup_p99
    // =======================================================================

    /// ConflictIndex lookup p99 under load: register 10K entries across 100 keys,
    /// then perform 10K lookups. p99 must stay below threshold.
    #[test]
    fn perf_conflict_index_lookup_p99() {
        let mut idx = ConflictIndex::new(100_000);

        // Register 10K entries across 100 keys (100 txns per key).
        for key_idx in 0..100u64 {
            for txn_idx in 0..100u64 {
                let t = key_idx * 1000 + txn_idx;
                let entry = InFlightWrite {
                    txn_id: TxnId(ts(t)),
                    t0: ts(t),
                    accord_ts: None,
                    status: TxnStatus::PreAccepted,
                };
                idx.register(format!("perf:k:{}", key_idx).as_bytes(), entry)
                    .unwrap();
            }
        }

        // Perform 10K lookups.
        let mut durations = Vec::new();
        for i in 0..10_000u64 {
            let key = format!("perf:k:{}", i % 100);
            let start = Instant::now();
            let _ = idx.deps_before_t0(key.as_bytes(), &ts(50_000));
            durations.push(start.elapsed().as_nanos());
        }

        durations.sort();
        let p99 = percentile(&durations, 99.0);
        // With 100 entries per key, deps_before_t0 scans the list.
        // 5ms threshold accommodates CI runners with llvm-cov instrumentation overhead.
        assert!(
            p99 < 5_000_000,
            "conflict index lookup p99 = {}ns exceeds 5ms threshold",
            p99
        );

        // Also verify correctness: a lookup returns the right number of deps.
        let key0_deps = idx.deps_before_t0(b"perf:k:0", &ts(50));
        // key 0 has txns at t=0..99. deps_before_t0(t0=50) returns entries
        // with t0 < 50, which is t=0..49.
        assert_eq!(
            key0_deps.len(),
            50,
            "expected 50 deps for key 0 before t=50"
        );
    }

    // =======================================================================
    // A7.9-T4: perf_reorder_buffer_overhead_p99
    // =======================================================================

    /// ReorderBuffer overhead: push 10K messages, drain in batches.
    /// Push p99 and drain-per-message overhead must stay below threshold.
    #[test]
    fn perf_reorder_buffer_overhead_p99() {
        let timing = TimingConfig {
            skew_max_us: 10_000,
            rtt_p99_us: 5_000,
        };
        let mut buf = ReorderBuffer::new(20_000, timing);

        // Push 10K messages with ascending t0.
        let mut push_durations = Vec::new();
        for i in 0..10_000i64 {
            let msg = Message {
                t0: i * 25, // spread across timeline
                payload: vec![(i & 0xFF) as u8],
            };
            let start = Instant::now();
            buf.push(msg).unwrap();
            push_durations.push(start.elapsed().as_nanos());
        }

        assert_eq!(buf.len(), 10_000, "all messages should be buffered");

        push_durations.sort();
        let push_p99 = percentile(&push_durations, 99.0);
        assert!(
            push_p99 < 100_000, // 100us
            "reorder buffer push p99 = {}ns exceeds 100us threshold",
            push_p99
        );

        // Drain in batches: advance time in steps.
        let mut drain_durations = Vec::new();
        let mut total_drained = 0;
        for batch in 0..100i64 {
            let now = (batch + 1) * 3000;
            let start = Instant::now();
            let ready = buf.drain_ready(now);
            let elapsed = start.elapsed().as_nanos();
            total_drained += ready.len();
            if !ready.is_empty() {
                // Per-message overhead.
                drain_durations.push(elapsed / ready.len() as u128);
            }
        }

        // Final drain to flush everything.
        let remaining = buf.drain_all();
        total_drained += remaining.len();

        assert_eq!(
            total_drained, 10_000,
            "all 10K messages must eventually be drained"
        );

        if !drain_durations.is_empty() {
            drain_durations.sort();
            let drain_p99 = percentile(&drain_durations, 99.0);
            assert!(
                drain_p99 < 100_000, // 100us per message
                "reorder buffer drain per-msg p99 = {}ns exceeds 100us",
                drain_p99
            );
        }
    }
}
