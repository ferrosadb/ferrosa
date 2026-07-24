//! B1 T1.6 — property tests for the fair-share scheduling logic (FM-4/FM-5).
//!
//! Drives the pure vruntime primitives ([`RunQueue`] + [`advance_vruntime`] —
//! exactly what [`FairAdmit`](ferrosa_sched::fair_admit::FairAdmit) uses live,
//! minus the slot accounting) over randomized scan populations and service
//! costs, asserting:
//!
//! * **Equal-weight convergence** — N equal-weight scans get equal service
//!   within an O(N) spread (never O(rounds) — that would be starvation).
//! * **Aging** — a long scan keeps making progress under a stream of arriving,
//!   higher-weight interactive scans; it is never starved in any window.

use ferrosa_sched::runqueue::{RunQueue, SchedEntity};
use ferrosa_sched::scheduler::advance_vruntime;
use ferrosa_sched::SchedClass;
use proptest::prelude::*;

/// Run `cur` for one chunk of `service`, then re-compete against `q`: if a
/// waiting scan is now more deserving, enqueue `cur` and take the smallest.
/// Mirrors `FairAdmit`'s per-chunk decision on the pure queue.
fn step(q: &mut RunQueue, cur: SchedEntity, service: u64) -> SchedEntity {
    let mut cur = cur;
    cur.vruntime = advance_vruntime(cur.vruntime, service, cur.weight);
    if q.peek_min_vruntime()
        .map(|m| m < cur.vruntime)
        .unwrap_or(false)
    {
        q.enqueue(cur);
        q.pick_next().expect("a scan to run")
    } else {
        cur
    }
}

proptest! {
    #[test]
    fn equal_weight_scans_converge_to_equal_service(
        n in 2u64..=6,
        rounds in 200usize..=1500,
        service in 1u64..=20,
    ) {
        let mut q = RunQueue::new(0);
        for id in 0..n {
            q.enqueue(SchedEntity::new(id, SchedClass::Bulk));
        }
        let mut ran = vec![0u64; n as usize];
        let mut cur = q.pick_next().expect("a scan to run");
        for _ in 0..rounds {
            ran[cur.id as usize] += 1;
            cur = step(&mut q, cur, service);
        }
        let max = *ran.iter().max().unwrap();
        let min = *ran.iter().min().unwrap();
        prop_assert!(
            max - min <= 2 * n,
            "unfair spread {} (max {max}, min {min}) for {n} equal-weight scans over {rounds}: {ran:?}",
            max - min
        );
    }

    #[test]
    fn long_scan_makes_progress_under_interactive_churn(
        rounds in 200usize..=1200,
        churn_every in 2usize..=5,
        service in 1u64..=10,
    ) {
        let mut q = RunQueue::new(0);
        q.enqueue(SchedEntity::new(0, SchedClass::Bulk)); // the long scan
        let mut long_progress = 0u64;
        let mut next_interactive = 1u64;
        let mut samples = Vec::new();
        let mut cur = q.pick_next().expect("the long scan");

        for round in 0..rounds {
            if round % churn_every == 0 {
                // A Foreground interactive scan arrives (competes, then leaves
                // once served — modeled by not re-enqueuing it below).
                q.enqueue(SchedEntity::new(next_interactive, SchedClass::Foreground));
                next_interactive += 1;
            }
            if cur.id == 0 {
                long_progress += 1;
            }
            // The long scan (id 0) always re-competes; an interactive scan (id>0)
            // completes after its turn, so we just take the next.
            cur = if cur.id == 0 {
                step(&mut q, cur, service)
            } else {
                // An interactive scan completes after its turn — discard it (it
                // does not re-enqueue) and take the next; the long scan is always
                // queued.
                let _done = cur;
                q.pick_next().expect("the long scan is always queued")
            };
            if round % 25 == 0 {
                samples.push(long_progress);
            }
        }

        prop_assert!(
            long_progress >= rounds as u64 / 4,
            "long scan starved: {long_progress} turns in {rounds} rounds"
        );
        for w in samples.windows(2) {
            prop_assert!(w[1] > w[0], "long scan stalled in a window: {samples:?}");
        }
    }
}

/// B2 T2.2 — service-time (elapsed) accounting throttles an I/O-bound scan to
/// its fair share. Both scans are equal-weight `Bulk`, but each of scan 0's
/// chunks costs `IO_COST` service (elapsed *including* S3 wait) while scan 1's
/// cost 1 (CPU-light). This is exactly what `ScanSlot::tick` now feeds
/// `reschedule`: the window's measured elapsed µs, not a chunk count. Charging
/// `vruntime` by service makes the two converge to equal TOTAL SERVICE — the
/// I/O-bound scan gets proportionally FEWER turns, so it cannot hog the pool by
/// being slow (the pre-T2.2 chunk-count accounting would have given it EQUAL
/// turns, i.e. `IO_COST`× more wall-time).
#[test]
fn service_time_accounting_equalizes_io_bound_and_cpu_light_scans() {
    const IO_COST: u64 = 50;
    let mut q = RunQueue::new(0);
    q.enqueue(SchedEntity::new(0, SchedClass::Bulk)); // I/O-bound
    q.enqueue(SchedEntity::new(1, SchedClass::Bulk)); // CPU-light
    let cost = |id: u64| if id == 0 { IO_COST } else { 1 };

    let mut service = [0u64; 2];
    let mut turns = [0u64; 2];
    let mut cur = q.pick_next().expect("a scan to run");
    for _ in 0..8000 {
        let c = cost(cur.id);
        service[cur.id as usize] += c;
        turns[cur.id as usize] += 1;
        cur = step(&mut q, cur, c);
    }

    // Equal weight + service accounting → total SERVICE converges within about
    // one chunk cost, regardless of how that service splits into turns.
    let spread = service[0].abs_diff(service[1]);
    assert!(
        spread <= IO_COST * 2,
        "service not equalized: {service:?} (spread {spread})"
    );
    // ...and the I/O-bound scan takes far fewer turns — throttled by its elapsed
    // cost rather than getting equal turns for burning no CPU.
    assert!(
        turns[1] > turns[0] * 10,
        "the CPU-light scan should get ~IO_COST× more turns: {turns:?}"
    );
}
