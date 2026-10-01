//! Load-independent measurement primitives for the Accord benchmarks.
//!
//! # Why these exist
//!
//! An ABSOLUTE wall-clock threshold assertion measures the RUNNER as much as
//! the code: on a shared, contended machine the scheduler steals the measured
//! thread and the "latency" is really the deschedule gap. That is not
//! hypothetical here — a contended merge-group build clocked a 1000-message
//! `ReorderBuffer` drain at 52.8 ms against a 10 ms bound and ejected a
//! docs-only PR from the merge queue (`forge t_430e21f7`), and on 2026-09-30
//! the same drain was ejected again at 56.7 ms while the dedicated
//! `perf-regression` job PASSED in the same workflow on the same commit.
//!
//! The repo rule is "there are no flaky tests, only flaky code" — so an
//! absolute bound that trips does not get widened. It either reveals a real
//! defect, or the bound was a load-coupled PROXY for a load-independent
//! property and must be rewritten as that property. This module provides the
//! rewrite primitive.
//!
//! # The statistic: minimum cost of one operation over a same-run reference loop
//!
//! A DESCHEDULED sample is the problem: it adds the scheduler's gap to the
//! op's cost. Aggregates accumulate those gaps — under load a 10_000-op total
//! ballooned to 100x its idle value, because *some* ops are always descheduled.
//! An ABSOLUTE aggregate bound therefore measures the load. The MINIMUM
//! single-operation cost does not: it is the cost of one op that ran without
//! being descheduled, and it tracks the CPU's throughput, not the scheduler's.
//! Measured on a shared 18-core box from load 0 to load 150, the minimum cost
//! of one `ConflictIndex::deps_before_t0` lookup held flat at 0.0008-0.0042
//! reference loops while its 10_000-op total grew from 0.9 to 600+ loops.
//!
//! Expressing that minimum as a multiple of a fixed scalar CPU loop measured in
//! the same run removes the machine's absolute speed too: the op and the
//! reference loop are both CPU-bound, so a slower/contended machine slows both
//! and the ratio is stable.
//!
//! The minimum still catches a real regression: a uniform slowdown (a clone
//! added to a hot path) and a superlinear one (`O(n^2)` scan) both raise the
//! cheapest op's cost. It does NOT catch a pure tail regression, which is why
//! precise tail measurement belongs on dedicated hardware (the Fly perf-tier
//! rigs) — an absolute p99 bound on a shared runner would only measure load.

use std::time::Instant;

/// A fixed, CPU-bound arithmetic loop whose cost depends only on how fast this
/// machine is executing scalar code right now.
///
/// The MINIMUM of five samples is returned: the least-descheduled sample is the
/// one that reflects the machine's actual throughput rather than the
/// scheduler's.
pub fn reference_loop_ns() -> u128 {
    let work = || {
        let mut acc: u64 = 0;
        for i in 0..40_000u64 {
            acc = acc
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(i ^ acc);
        }
        acc
    };
    (0..5)
        .map(|_| {
            let start = Instant::now();
            std::hint::black_box(work());
            start.elapsed().as_nanos()
        })
        .min()
        .expect("at least one sample")
}

/// Run `f` once and return its elapsed wall time in nanoseconds. The result is
/// black-boxed so the call is not optimised away.
pub fn time_ns<R>(f: impl FnOnce() -> R) -> u128 {
    let start = Instant::now();
    let out = f();
    let elapsed = start.elapsed().as_nanos();
    std::hint::black_box(out);
    elapsed
}

/// The cheapest single-operation cost, as a multiple of the same-run reference
/// loop.
///
/// `measure` runs the benchmark once and returns the SMALLEST single-operation
/// wall time, in nanoseconds, that it observed. This function calls it `reps`
/// times and keeps the overall minimum, so a single descheduled sample cannot
/// move the result. Returns `(best_op_ns, ratio, reference_loop_ns)`.
pub fn best_min_op_ratio(
    name: &str,
    reps: usize,
    mut measure: impl FnMut() -> u128,
) -> (u128, f64, u128) {
    assert!(reps > 0, "{name}: reps must be > 0");

    let reference_ns = reference_loop_ns();
    assert!(reference_ns > 0, "{name}: reference loop measured zero");

    let mut best_op = u128::MAX;
    for _ in 0..reps {
        best_op = best_op.min(measure());
    }
    assert!(best_op != u128::MAX, "{name}: measure produced no sample");

    let ratio = best_op as f64 / reference_ns as f64;
    (best_op, ratio, reference_ns)
}

/// Assert that the cheapest single operation costs no more than `budget`
/// reference loops (see [`best_min_op_ratio`]). Returns the best op time in ns.
pub fn assert_min_op_in_reference_loops(
    name: &str,
    budget: f64,
    reps: usize,
    measure: impl FnMut() -> u128,
) -> u128 {
    assert!(
        budget.is_finite() && budget > 0.0,
        "{name}: budget must be a positive finite ratio"
    );
    let (best_op, ratio, reference_ns) = best_min_op_ratio(name, reps, measure);
    assert!(
        ratio <= budget,
        "{name}: cheapest op = {best_op}ns = {ratio:.4} reference loops; \
         budget is {budget:.4} (a reference loop is {reference_ns}ns on this \
         machine). This is a same-run ratio of the MINIMUM op cost, so it \
         cannot be tripped by machine load — it measures the code, not the \
         runner"
    );
    best_op
}
