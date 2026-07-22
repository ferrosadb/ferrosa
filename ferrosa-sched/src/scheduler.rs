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

use std::sync::{Arc, Mutex};

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

    /// Take the most-deserving waiting unit as a running [`SchedTicket`] bound to
    /// this scheduler (`None` if idle). The scan holds the ticket while it runs
    /// and calls [`SchedTicket::reschedule`] per chunk.
    pub fn pick_ticket(self: &Arc<Self>) -> Option<SchedTicket> {
        self.pick_next().map(|entity| SchedTicket {
            scheduler: Arc::clone(self),
            entity,
        })
    }
}

/// A running unit's scheduling handle — the fair-share accounting the store scan
/// page loop drives. Holds the unit's [`SchedEntity`] and a handle back to its
/// [`Scheduler`]. The store producer calls [`reschedule`](Self::reschedule) once
/// per produced chunk; when it returns `true` the producer releases its pool
/// slot and [`yield_back`](Self::yield_back)s so a more-deserving scan can run.
#[derive(Debug)]
pub struct SchedTicket {
    scheduler: Arc<Scheduler>,
    entity: SchedEntity,
}

impl SchedTicket {
    /// The scheduled unit's id.
    pub fn id(&self) -> u64 {
        self.entity.id
    }

    /// The unit's accumulated virtual runtime.
    pub fn vruntime(&self) -> u64 {
        self.entity.vruntime
    }

    /// The unit's fair-share weight.
    pub fn weight(&self) -> u32 {
        self.entity.weight
    }

    /// Account `service` for the work just done and report whether this unit
    /// should yield its slot to a more-deserving waiter. Does NOT itself release
    /// any pool permit — the caller does that after [`yield_back`](Self::yield_back)
    /// so no resource is held across the handoff (T1.7).
    pub fn reschedule(&mut self, service: u64) -> bool {
        self.scheduler.reschedule(&mut self.entity, service)
    }

    /// Yield the slot: re-enqueue this unit for a future turn (consumes the
    /// ticket). The caller picks the next ticket after releasing its pool permit.
    pub fn yield_back(self) {
        self.scheduler.requeue(self.entity);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// A mock scan: produces `remaining` chunks. Stands in for a `store.rs`
    /// `range_iter` producer without any real storage. (Keyed by id in the map.)
    struct MockScan {
        remaining: u64,
        class: SchedClass,
    }

    /// T1.2 T2 — the MockStorage multi-scan fairness property: with a single pool
    /// slot (capacity 1), three concurrent scans driven through the ticket API
    /// interleave — no scan monopolizes the slot — and every scan completes.
    #[test]
    fn mock_storage_scans_share_the_slot_fairly_via_tickets() {
        let sched = Arc::new(Scheduler::new(0));
        let page_service = 10u64;

        let mut scans: HashMap<u64, MockScan> = HashMap::new();
        for id in [1u64, 2, 3] {
            sched.admit(id, SchedClass::Bulk);
            scans.insert(
                id,
                MockScan {
                    remaining: 100,
                    class: SchedClass::Bulk,
                },
            );
        }

        let mut ran: HashMap<u64, u64> = HashMap::new();
        let mut last = 0u64;
        let mut streak = 0u64;
        let mut max_streak_while_contended = 0u64;

        let mut ticket = sched.pick_ticket().expect("a scan to run");
        loop {
            let id = ticket.id();
            // "Produce" one chunk of scan `id`.
            *ran.entry(id).or_insert(0) += 1;
            let scan = scans.get_mut(&id).expect("known scan");
            scan.remaining -= 1;
            let done = scan.remaining == 0;
            let _ = scan.class; // (class would seed the weight in the real wiring)

            // Only measure monopolization while >= 2 scans still have work — the
            // lone tail necessarily runs alone and is not "monopolizing".
            let contended = scans.values().filter(|s| s.remaining > 0).count() >= 2;
            if id == last {
                streak += 1;
            } else {
                streak = 1;
                last = id;
            }
            if contended {
                max_streak_while_contended = max_streak_while_contended.max(streak);
            }

            let should_yield = ticket.reschedule(page_service);
            if done {
                match sched.pick_ticket() {
                    Some(t) => ticket = t,
                    None => break,
                }
                last = 0;
                streak = 0;
            } else if should_yield {
                ticket.yield_back();
                ticket = sched.pick_ticket().expect("a scan to run");
            }
            // else: keep running the same ticket (nobody more deserving waits).
        }

        // Every scan ran all 100 chunks.
        assert_eq!(ran[&1], 100);
        assert_eq!(ran[&2], 100);
        assert_eq!(ran[&3], 100);
        // No scan monopolized the slot while others had work.
        assert!(
            max_streak_while_contended <= 2,
            "a scan monopolized the slot: max consecutive run = {max_streak_while_contended}"
        );
    }

    #[test]
    fn ticket_reports_accounting_and_yields_back_to_the_queue() {
        let sched = Arc::new(Scheduler::new(0));
        // Foreground (weight 1024) so service advances vruntime one-for-one.
        sched.admit(1, SchedClass::Foreground);
        sched.admit(2, SchedClass::Foreground);
        let mut t = sched.pick_ticket().unwrap();
        assert_eq!(t.id(), 1);
        assert_eq!(t.vruntime(), 0);
        // Accounting advances vruntime and, with a waiter at 0, signals yield.
        assert!(t.reschedule(10));
        assert_eq!(t.vruntime(), 10);
        assert_eq!(sched.waiting(), 1); // only id 2 waiting
        t.yield_back();
        assert_eq!(sched.waiting(), 2); // id 1 back in the queue
        assert_eq!(sched.pick_ticket().unwrap().id(), 2); // id 2 now most deserving
    }

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
