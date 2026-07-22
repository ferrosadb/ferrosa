//! Module: Fair-share scheduler core — virtual-runtime accounting and the
//!   yield decision that keeps concurrent scans from monopolizing the pool.
//! Correctness: Correct when `advance_vruntime` charges service inversely to
//!   weight, `should_switch` yields exactly when a waiting unit is more
//!   deserving (strictly smaller vruntime), and a multi-unit interleave gives
//!   each equal-weight unit a fair share of service.
//! Last revised: 2026-07-22
//! Last changed: New module — B1 T1.2 core. Sits on the [`RunQueue`](crate::runqueue)
//!   (T1.1). `SchedTicket::reschedule` (and the store.rs page loop that calls it)
//!   are wired on top of this in the T1.2 integration step.
//!
//! # Accounting
//!
//! A unit's `vruntime` advances by `service * BASE_WEIGHT / weight`. A
//! full-weight (Foreground, 1024) unit advances one-for-one with real service; a
//! quarter-weight (Bulk, 256) unit advances 4x faster, so it drifts to the back
//! of the queue and cedes the pool to interactive work. Because the queue always
//! runs the smallest-vruntime unit, equal-weight units converge to equal
//! service and unequal-weight units to service proportional to weight.

use std::sync::Mutex;

use crate::runqueue::{RunQueue, SchedEntity};
use crate::SchedClass;

/// Reference fair-share weight (a Foreground unit). Service for a unit of this
/// weight advances `vruntime` one-for-one.
pub const BASE_WEIGHT: u64 = 1024;

/// Advance `vruntime` by `service` charged inversely to `weight`. Pure.
///
/// `delta = service * BASE_WEIGHT / weight`. Saturating so a pathological
/// service value can never wrap the counter.
pub fn advance_vruntime(vruntime: u64, service: u64, weight: u32) -> u64 {
    let weight = u64::from(weight.max(1));
    let delta = service.saturating_mul(BASE_WEIGHT) / weight;
    vruntime.saturating_add(delta)
}

/// Whether a unit at `running_vruntime` should cede the slot to the smallest
/// waiting unit. Pure. Yields only when a waiting unit is STRICTLY more
/// deserving, so equal-vruntime units don't thrash back and forth.
pub fn should_switch(running_vruntime: u64, min_waiting_vruntime: Option<u64>) -> bool {
    match min_waiting_vruntime {
        Some(waiting) => waiting < running_vruntime,
        None => false,
    }
}

/// A single-group fair-share scheduler wrapping a [`RunQueue`] behind a mutex.
///
/// The lock is held only for the O(log n) queue operations — never across the
/// caller's service work or an `.await` (T1.7 audits this) — so it does not
/// serialize scan execution.
#[derive(Debug)]
pub struct Scheduler {
    queue: Mutex<RunQueue>,
}

impl Scheduler {
    /// A scheduler whose waking units get `boost` sleeper credit (see
    /// [`RunQueue::new`]).
    pub fn new(boost: u64) -> Self {
        Self {
            queue: Mutex::new(RunQueue::new(boost)),
        }
    }

    /// Admit a fresh unit of `class` under `id`, entering at the queue floor.
    pub fn admit(&self, id: u64, class: SchedClass) {
        let mut q = self.queue.lock().expect("scheduler queue poisoned");
        q.enqueue(SchedEntity::new(id, class));
    }

    /// Take the most-deserving waiting unit to run next (`None` if idle).
    pub fn pick_next(&self) -> Option<SchedEntity> {
        self.queue
            .lock()
            .expect("scheduler queue poisoned")
            .pick_next()
    }

    /// Account `service` for the `running` unit and report whether it should
    /// yield to a more-deserving waiting unit. Does NOT re-enqueue `running` —
    /// the caller decides that after releasing any resources it holds (T1.7).
    pub fn reschedule(&self, running: &mut SchedEntity, service: u64) -> bool {
        running.vruntime = advance_vruntime(running.vruntime, service, running.weight);
        let min_waiting = self
            .queue
            .lock()
            .expect("scheduler queue poisoned")
            .peek_min_vruntime();
        should_switch(running.vruntime, min_waiting)
    }

    /// Re-enqueue a running unit that is yielding.
    pub fn requeue(&self, entity: SchedEntity) {
        self.queue
            .lock()
            .expect("scheduler queue poisoned")
            .enqueue(entity);
    }

    /// Number of waiting units.
    pub fn waiting(&self) -> usize {
        self.queue.lock().expect("scheduler queue poisoned").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_charges_service_inversely_to_weight() {
        // Foreground (full weight): one-for-one.
        assert_eq!(advance_vruntime(0, 100, 1024), 100);
        // Bulk (quarter weight): 4x faster.
        assert_eq!(advance_vruntime(0, 100, 256), 400);
        // Zero weight is treated as 1 (no divide-by-zero).
        assert_eq!(advance_vruntime(0, 1, 0), 1024);
    }

    #[test]
    fn advance_saturates_instead_of_wrapping() {
        assert_eq!(advance_vruntime(u64::MAX - 1, 1000, 1024), u64::MAX);
    }

    #[test]
    fn should_switch_only_for_strictly_smaller_waiter() {
        assert!(should_switch(100, Some(99))); // waiter more deserving -> yield
        assert!(!should_switch(100, Some(100))); // equal -> no thrash
        assert!(!should_switch(100, Some(101))); // waiter less deserving
        assert!(!should_switch(100, None)); // nobody waiting
    }

    /// The load-bearing T1.2 property: two equal-weight scans interleave — over
    /// many pages neither monopolizes the pool; service is shared within ε.
    #[test]
    fn two_equal_weight_scans_interleave_fairly() {
        let sched = Scheduler::new(0);
        sched.admit(1, SchedClass::Bulk);
        sched.admit(2, SchedClass::Bulk);

        let page_service = 10u64;
        let mut ran = std::collections::HashMap::new();
        let mut current = sched.pick_next().expect("a unit to run");

        for _ in 0..1000 {
            *ran.entry(current.id).or_insert(0u64) += 1;
            if sched.reschedule(&mut current, page_service) {
                // Yield: re-enqueue self, pick the more-deserving unit.
                sched.requeue(current);
                current = sched.pick_next().expect("a unit to run");
            }
        }

        let a = ran[&1];
        let b = ran[&2];
        // Neither monopolizes: each ran a near-equal share of the 1000 pages.
        let diff = a.abs_diff(b);
        assert!(
            diff <= 2,
            "unfair interleave: id1={a} id2={b} (diff {diff})"
        );
    }

    /// A heavier (Foreground) unit gets proportionally more service than a Bulk
    /// unit sharing the pool — ~4:1 by weight.
    #[test]
    fn higher_weight_unit_gets_proportionally_more_service() {
        let sched = Scheduler::new(0);
        sched.admit(1, SchedClass::Foreground); // weight 1024
        sched.admit(2, SchedClass::Bulk); // weight 256

        let page_service = 10u64;
        let mut ran = std::collections::HashMap::new();
        let mut current = sched.pick_next().unwrap();
        for _ in 0..1000 {
            *ran.entry(current.id).or_insert(0u64) += 1;
            if sched.reschedule(&mut current, page_service) {
                sched.requeue(current);
                current = sched.pick_next().unwrap();
            }
        }
        let fg = ran[&1] as f64;
        let bulk = ran[&2] as f64;
        // Foreground should get ~4x the Bulk service (weight ratio 1024:256).
        let ratio = fg / bulk;
        assert!(
            (3.0..=5.0).contains(&ratio),
            "expected ~4:1 fg:bulk service, got {ratio:.2} (fg={fg}, bulk={bulk})"
        );
    }
}
