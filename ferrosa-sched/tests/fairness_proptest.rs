//! B1 T1.6 — property tests for the fair-share scheduler (FM-4/FM-5).
//!
//! Two invariants, over randomized scan populations and service costs:
//!
//! * **Equal-weight convergence** — N equal-weight scans sharing the pool get
//!   equal service within a small constant spread (O(N), never O(rounds) — that
//!   would be starvation).
//! * **Aging** — a long-running scan keeps making progress even under a steady
//!   stream of arriving-and-finishing interactive queries; it is never starved
//!   in any window.

use std::sync::Arc;

use ferrosa_sched::scheduler::Scheduler;
use ferrosa_sched::SchedClass;
use proptest::prelude::*;

proptest! {
    /// Equal-weight scans converge to equal service — the spread stays O(N),
    /// independent of how many scheduling rounds run.
    #[test]
    fn equal_weight_scans_converge_to_equal_service(
        n in 2u64..=6,
        rounds in 200usize..=1500,
        service in 1u64..=20,
    ) {
        let sched = Arc::new(Scheduler::new(0));
        for id in 0..n {
            sched.admit(id, SchedClass::Bulk);
        }

        let mut ran = vec![0u64; n as usize];
        let mut cur = sched.pick_ticket().expect("a scan to run");
        for _ in 0..rounds {
            ran[cur.id() as usize] += 1;
            if cur.reschedule(service) {
                cur.yield_back();
                cur = sched.pick_ticket().expect("a scan to run");
            }
        }

        let max = *ran.iter().max().unwrap();
        let min = *ran.iter().min().unwrap();
        // Fair share: bounded spread (the transient ordering + at most one extra
        // turn per scan). Crucially NOT proportional to `rounds`.
        prop_assert!(
            max - min <= 2 * n,
            "unfair service spread {} (max {max}, min {min}) for {n} equal-weight scans over {rounds} rounds: {ran:?}",
            max - min
        );
    }

    /// A long scan makes monotonic progress under interactive churn — it is
    /// never starved, even as higher-weight interactive queries keep arriving.
    #[test]
    fn long_scan_makes_progress_under_interactive_churn(
        rounds in 200usize..=1200,
        churn_every in 2usize..=5,
        service in 1u64..=10,
    ) {
        let sched = Arc::new(Scheduler::new(0));
        sched.admit(0, SchedClass::Bulk); // the long scan; id 0

        let mut long_progress = 0u64;
        let mut next_interactive = 1u64;
        let mut samples = Vec::new();
        let mut cur = sched.pick_ticket().expect("the long scan");

        for round in 0..rounds {
            // Interactive (Foreground) queries keep arriving.
            if round % churn_every == 0 {
                sched.admit(next_interactive, SchedClass::Foreground);
                next_interactive += 1;
            }

            let is_long = cur.id() == 0;
            if is_long {
                long_progress += 1;
            }
            let yielded = cur.reschedule(service);

            if is_long {
                // The long scan never finishes: keep running until it yields.
                if yielded {
                    cur.yield_back();
                    cur = sched.pick_ticket().expect("someone to run");
                }
            } else {
                // An interactive query completes after its turn; the long scan
                // is always still queued, so there is always someone to run.
                cur = sched.pick_ticket().expect("the long scan is always queued");
            }

            if round % 25 == 0 {
                samples.push(long_progress);
            }
        }

        // Not starved: the long scan got a fair fraction of turns despite the
        // churn (even weighted 4:1 against it, it still runs regularly).
        prop_assert!(
            long_progress >= rounds as u64 / 4,
            "long scan starved: {long_progress} turns in {rounds} rounds (churn every {churn_every})"
        );
        // Progress in every window — it never stalls for a whole 25-round span.
        for w in samples.windows(2) {
            prop_assert!(
                w[1] > w[0],
                "long scan made no progress in a window: {samples:?}"
            );
        }
    }
}
