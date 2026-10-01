//! Performance regression test suite for Accord.
//!
//! Every bound in this file is a same-run RATIO against a CPU reference loop,
//! never an absolute wall-clock figure. An absolute bound measures the runner:
//! a contended CI box deschedules the measured thread and the "latency" is the
//! deschedule gap, not the code.
//!
//! # Where these run
//!
//! NOT in the per-PR / merge-queue lane. `ci.yml` skips `accord::perf_regression`
//! and the suite runs nightly (`nightly-fuzz.yml`, job `perf-regression`).
//!
//! # Why no bound here is absolute
//!
//! This module originally asserted absolute bounds (`PreAccept < 1 ms`,
//! `Commit < 1 ms`, `ConflictIndex lookup < 100 us`, `ReorderBuffer push < 100 us`,
//! `multi-key txn < 10 ms`, `conflict index lookup < 5 ms`, `drain < 10 ms`,
//! `reorder-buffer drain per-msg < 100 us`). The drain bound ejected a docs-only
//! PR from the merge queue when a contended merge-group build clocked it at
//! 52.8 ms against its 10 ms bound (forge t_430e21f7); on 2026-09-30 the nightly
//! fuzz lane ejected it again at 56.7 ms while the dedicated `perf-regression`
//! job PASSED in the very same workflow — same commit, same code, only runner
//! load differing.
//!
//! Re-measured on a shared box at load 0 → 150, the same story held for every
//! one of them: the per-op cost stayed put while a descheduled sample made the
//! reported "latency" jump. `ConflictIndex::deps_before_t0` (the 5 ms bound)
//! measured 5.5 ms under contention; its cheapest op never moved. Every bound
//! was a nanosecond PROXY for a cost property that is itself load-independent,
//! so every one is now asserted directly as the cost of the MINIMUM operation,
//! over a same-run CPU reference loop (`perf_support`).
//!
//! The measured baselines and the budgets chosen for them:
//!
//! | benchmark                              | baseline (ref loops) | budget |
//! |----------------------------------------|----------------------|--------|
//! | PreAccept (single-key)                 | 0.017                | 0.5    |
//! | Commit                                 | 0.007                | 0.25   |
//! | ConflictIndex::max_conflicting_ts      | 0.0012               | 0.05   |
//! | ConflictIndex::deps_before_t0 (100/key)| 0.004                | 0.10   |
//! | ReorderBuffer push (1000 msgs)         | 0.0008               | 0.05   |
//! | ReorderBuffer push (10000 msgs)        | 0.0007               | 0.05   |
//! | ReorderBuffer drain (1000 msgs)        | 0.75                 | 6.0    |
//! | ReorderBuffer drain per-msg (10K)      | 0.0009               | 0.05   |
//! | multi-key txn (3 shards)               | 0.02                 | 0.5    |
//!
//! Each baseline is the min-op measurement on a shared 18-core box, load 0-150
//! (it did not move with load); each budget adds 10-30x headroom for a slower
//! machine or build. A real regression of any of these paths — a clone on a hot
//! path, an `O(n^2)` scan — is a multiple of the budget, not a few percent.
//! None of the old nanosecond figures was widened; they were replaced.
//!
//! The deterministic half of every benchmark is preserved: the exact results,
//! ordering and completeness each bound was never about are still asserted
//! alongside the timing, so a faster-but-wrong implementation fails.
//!
//! # A7.9 Tests
//!
//! - `perf_regression_suite` — all benchmarks within their reference-loop budget
//! - `perf_multi_key_txn_p50` — multi-key transaction cost
//! - `perf_conflict_index_lookup_p99` — conflict index lookup cost
//! - `perf_reorder_buffer_overhead_p99` — reorder buffer overhead

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use ferrosa_common::accord::{BallotNumber, Timestamp as AccordTimestamp, TxnId, TxnPhase};
    use ferrosa_storage::accord::conflict_index::{ConflictIndex, InFlightWrite, TxnStatus};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::perf_support::{assert_min_op_in_reference_loops, best_min_op_ratio};
    use crate::accord::reorder_buffer::{Message, ReorderBuffer, TimingConfig};
    use crate::accord::state_machine::{AccordStateMachine, SmResponse};
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

    fn timing() -> TimingConfig {
        TimingConfig {
            skew_max_us: 10_000,
            rtt_p99_us: 5_000,
        }
    }

    fn in_flight(t: u64) -> InFlightWrite {
        InFlightWrite {
            txn_id: TxnId(ts(t)),
            t0: ts(t),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        }
    }

    fn msg(t0: i64, payload: u8) -> Message {
        Message {
            t0,
            payload: vec![payload],
        }
    }

    /// A `ConflictIndex` with 1000 keys, one entry each.
    fn conflict_index_1000_keys() -> ConflictIndex {
        let mut idx = ConflictIndex::new(100_000);
        for i in 0..1000u64 {
            idx.register(format!("k:{i}").as_bytes(), in_flight(i))
                .unwrap();
        }
        idx
    }

    /// A `ConflictIndex` with 100 keys and 100 entries per key.
    fn conflict_index_100x100() -> ConflictIndex {
        let mut idx = ConflictIndex::new(100_000);
        for key_idx in 0..100u64 {
            for txn_idx in 0..100u64 {
                let t = key_idx * 1000 + txn_idx;
                idx.register(format!("perf:k:{key_idx}").as_bytes(), in_flight(t))
                    .unwrap();
            }
        }
        idx
    }

    /// A `ReorderBuffer` holding `n` messages with distinct ascending `t0`.
    fn reorder_buffer_with(n: i64) -> ReorderBuffer {
        let mut buf = ReorderBuffer::new(n as usize + 1, timing());
        for i in 0..n {
            buf.push(msg(i * 100, (i & 0xFF) as u8)).unwrap();
        }
        buf
    }

    // =======================================================================
    // A7.9-T1: perf_regression_suite
    // =======================================================================

    /// All benchmarks stay within their reference-loop budgets:
    /// - Single-key PreAccept
    /// - Commit
    /// - ConflictIndex lookup
    /// - ReorderBuffer push
    /// - ReorderBuffer drain (1000 msgs)
    ///
    /// Each budget is a same-run ratio against a CPU reference loop, NOT an
    /// absolute nanosecond bound (see the module docs).
    #[test]
    fn perf_regression_suite() {
        // --- Single-key PreAccept benchmark ---
        // Budget: 0.5 reference loops (baseline ~0.017). A PreAccept that grows
        // a clone of the write-set or an O(keys) scan is a multiple.
        let _ = assert_min_op_in_reference_loops("PreAccept (single-key)", 0.5, 5, || {
            let mut sm = AccordStateMachine::new(1, Arc::new(MockSyncWriter::new()));
            let mut min = u128::MAX;
            for i in 0..100u64 {
                let tid = txn(1, 1000 + i);
                let start = Instant::now();
                let resp = sm.handle_preaccept(tid, ts(1000 + i), b"key", BallotNumber(0), 0);
                let dt = start.elapsed().as_nanos();
                // Deterministic half: every PreAccept must agree.
                match resp {
                    SmResponse::PreAcceptOK { txn_id, .. } => assert_eq!(txn_id, tid),
                    other => panic!("PreAccept must return PreAcceptOK, got {other:?}"),
                }
                min = min.min(dt);
            }
            min
        });

        // --- Commit benchmark ---
        // Budget: 0.25 reference loops (baseline ~0.007). PreAccept + Accept is
        // SETUP, so it is excluded from the timed samples and the bound stays on
        // Commit itself, exactly what the original measured.
        let _ = assert_min_op_in_reference_loops("Commit", 0.25, 5, || {
            let mut sm = AccordStateMachine::new(2, Arc::new(MockSyncWriter::new()));
            let mut min = u128::MAX;
            for i in 0..100u64 {
                let tid = txn(2, 2000 + i);
                let t0 = ts(2000 + i);
                sm.handle_preaccept(tid, t0, b"ckey", BallotNumber(0), 0);
                sm.handle_accept(tid, t0, ts(2001 + i), vec![], BallotNumber(1));
                let start = Instant::now();
                sm.handle_commit(tid, t0, ts(2001 + i), vec![]);
                min = min.min(start.elapsed().as_nanos());
            }
            min
        });

        // --- ConflictIndex lookup benchmark ---
        // Budget: 0.05 reference loops (baseline ~0.0012).
        let _ = assert_min_op_in_reference_loops(
            "ConflictIndex::max_conflicting_timestamp",
            0.05,
            5,
            || {
                let idx = conflict_index_1000_keys();
                let mut min = u128::MAX;
                for i in 0..1000u64 {
                    let key = format!("k:{i}");
                    let start = Instant::now();
                    let found = idx.max_conflicting_timestamp(key.as_bytes());
                    let dt = start.elapsed().as_nanos();
                    // Deterministic half: the lookup must find the entry's ts.
                    assert_eq!(
                        found,
                        Some(ts(i)),
                        "lookup for {key} must return its registered timestamp"
                    );
                    min = min.min(dt);
                }
                min
            },
        );

        // --- ReorderBuffer push benchmark ---
        // Budget: 0.05 reference loops (baseline ~0.0008).
        let _ = assert_min_op_in_reference_loops("ReorderBuffer push (1000 msgs)", 0.05, 5, || {
            let mut buf = ReorderBuffer::new(10_000, timing());
            let mut min = u128::MAX;
            for i in 0..1000i64 {
                let start = Instant::now();
                buf.push(msg(i * 100, (i & 0xFF) as u8)).unwrap();
                min = min.min(start.elapsed().as_nanos());
            }
            // Deterministic half: every message must be buffered.
            assert_eq!(buf.len(), 1000);
            min
        });

        // --- ReorderBuffer drain benchmark (1000 msgs) ---
        // Budget: 6.0 reference loops for the whole batch (baseline ~0.75) —
        // the load-independent form of the original 10 ms drain bound.
        let _ = assert_min_op_in_reference_loops("ReorderBuffer drain (1000 msgs)", 6.0, 5, || {
            let mut buf = reorder_buffer_with(1000);
            let start = Instant::now();
            let ready = buf.drain_ready(i64::MAX);
            let dt = start.elapsed().as_nanos();
            // Deterministic half: nothing lost, and ordered by t0.
            assert_eq!(ready.len(), 1000);
            assert!(ready.windows(2).all(|w| w[0].t0 <= w[1].t0));
            dt
        });
    }

    // =======================================================================
    // A7.9-T2: perf_multi_key_txn_p50
    // =======================================================================

    /// Multi-key transaction cost: a 3-shard transaction through `TestCluster`
    /// must stay within its reference-loop budget.
    #[test]
    fn perf_multi_key_txn_p50() {
        // Budget: 0.5 reference loops (baseline ~0.02) for a full 3-shard
        // PreAccept+Commit.
        let tid = txn(1, 10_000);
        let t0 = ts(10_000);
        let _ = assert_min_op_in_reference_loops("multi-key txn (3 shards)", 0.5, 5, || {
            let mut cluster = TestCluster::new(3);
            let start = Instant::now();

            for (key_idx, &r) in [1u64, 2, 3].iter().enumerate() {
                cluster.send(TestMessage {
                    src: 1,
                    dst: r,
                    payload: TestMessagePayload::PreAccept {
                        txn_id: tid,
                        t0,
                        key: format!("multi:key:{key_idx}").into_bytes(),
                    },
                });
            }
            cluster.drain();

            for &r in &[1u64, 2, 3] {
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
            let dt = start.elapsed().as_nanos();

            // Deterministic half: the transaction must be committed on every
            // replica (a faster-but-wrong run must still fail).
            for r in &cluster.replicas {
                let state = r
                    .txn_states
                    .get(&tid)
                    .unwrap_or_else(|| panic!("replica {} saw no state", r.node_id));
                assert_eq!(
                    state.phase,
                    TxnPhase::Committed,
                    "replica {} must have committed the txn",
                    r.node_id
                );
            }
            dt
        });
    }

    // =======================================================================
    // A7.9-T3: perf_conflict_index_lookup_p99
    // =======================================================================

    /// Conflict index `deps_before_t0` cost: with 100 entries per key it scans
    /// the per-key list. Cost must stay within its reference-loop budget.
    #[test]
    fn perf_conflict_index_lookup_p99() {
        // Budget: 0.10 reference loops (baseline ~0.004). The original 5 ms
        // bound was ~70x the measured cost and still tripped under load
        // (measured 5.5 ms) because a single deschedule was mistaken for the
        // code being slow. A per-key scan that becomes superlinear is a
        // multiple of this budget.
        let _ = assert_min_op_in_reference_loops(
            "ConflictIndex::deps_before_t0 (100 entries/key)",
            0.10,
            5,
            || {
                let idx = conflict_index_100x100();
                let mut min = u128::MAX;
                let mut deps_seen = 0usize;
                for i in 0..10_000u64 {
                    let key = format!("perf:k:{}", i % 100);
                    let start = Instant::now();
                    let deps = idx.deps_before_t0(key.as_bytes(), &ts(50_000));
                    let dt = start.elapsed().as_nanos();
                    deps_seen += deps.len();
                    min = min.min(dt);
                }
                // Deterministic half: each of the 100 keys has entries at
                // t = key*1000 + 0..99; deps before 50_000 is 50 per key, so
                // 10_000 lookups must see 500_000 deps in total.
                assert_eq!(deps_seen, 500_000, "every key must scan its 50 deps");
                min
            },
        );

        // Correctness: a lookup returns the right deps.
        let mut idx = ConflictIndex::new(100_000);
        for txn_idx in 0..100u64 {
            idx.register(b"perf:k:0", in_flight(txn_idx)).unwrap();
        }
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

    /// ReorderBuffer overhead: push 10K messages, then drain in batches. Push
    /// cost and per-message drain cost must stay within their reference-loop
    /// budgets.
    #[test]
    fn perf_reorder_buffer_overhead_p99() {
        // Push budget: 0.05 reference loops (baseline ~0.0007).
        let _ =
            assert_min_op_in_reference_loops("ReorderBuffer push (10000 msgs)", 0.05, 5, || {
                let mut buf = ReorderBuffer::new(20_000, timing());
                let mut min = u128::MAX;
                for i in 0..10_000i64 {
                    let start = Instant::now();
                    buf.push(msg(i * 25, (i & 0xFF) as u8)).unwrap();
                    min = min.min(start.elapsed().as_nanos());
                }
                // Deterministic half: all 10K messages buffered.
                assert_eq!(buf.len(), 10_000, "all messages should be buffered");
                min
            });

        // Per-message drain budget: 0.05 reference loops (baseline ~0.0009).
        let (best_op, per_msg, reference_ns) =
            best_min_op_ratio("ReorderBuffer drain per-msg (10K)", 5, || {
                let mut buf = ReorderBuffer::new(20_000, timing());
                for i in 0..10_000i64 {
                    buf.push(msg(i * 25, (i & 0xFF) as u8)).unwrap();
                }
                let mut min = u128::MAX;
                let mut total_drained = 0;
                for batch in 0..100i64 {
                    let now = (batch + 1) * 3000;
                    let start = Instant::now();
                    let ready = buf.drain_ready(now);
                    let per_msg = start.elapsed().as_nanos() / ready.len().max(1) as u128;
                    total_drained += ready.len();
                    if !ready.is_empty() {
                        min = min.min(per_msg);
                    }
                }
                total_drained += buf.drain_all().len();
                // Deterministic half: every message must be drained, exactly
                // once, across the batched drains and the final flush.
                assert_eq!(
                    total_drained, 10_000,
                    "all 10K messages must eventually be drained"
                );
                min
            });
        assert!(
            per_msg <= 0.05,
            "reorder buffer drain per-msg = {:.4} reference loops (cheapest op \
             {best_op}ns, reference loop {reference_ns}ns); budget is 0.0500. \
             This is a same-run ratio of the MINIMUM op cost, so it cannot be \
             tripped by machine load",
            per_msg
        );
    }
}
