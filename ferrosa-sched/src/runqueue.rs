//! Module: A CFS-inspired fair-share run queue for background scan units.
//! Correctness: Correct when `pick_next` always returns the least-`vruntime`
//!   entity (FIFO among ties), `enqueue` clamps a waking entity's `vruntime` to
//!   no less than `min_vruntime - boost`, and `min_vruntime` is monotonic
//!   non-decreasing across every operation.
//! Last revised: 2026-07-22
//! Last changed: New module — B1 T1.1. The bounded pool (B0) stops scans from
//!   starving consensus; this run queue makes concurrent scans share the pool's
//!   slots *fairly* (no single full-table scan monopolizes) via virtual-runtime
//!   ordering. T1.2 wires service-time accounting (`reschedule`) onto it.
//!
//! # Model (Linux CFS, adapted)
//!
//! Each schedulable unit carries a `vruntime` — virtual runtime, i.e. service
//! time scaled by the inverse of its fair-share `weight`. The queue always runs
//! the entity that has received the *least* virtual service so far (smallest
//! `vruntime`), which equalizes weighted CPU over time.
//!
//! `min_vruntime` is a monotonic floor tracking the least vruntime seen. A unit
//! that was blocked (e.g. waiting on I/O) and re-enqueues must not be able to
//! undercut running units by an unbounded amount — otherwise a long-sleeping
//! scan would monopolize the pool on wake. So `enqueue` clamps its vruntime up
//! to at least `min_vruntime - boost`; `boost` is the bounded sleeper credit
//! that keeps a just-woken interactive unit responsive without letting it starve
//! others.

use std::collections::BTreeMap;

use crate::SchedClass;

/// Fair-share weight of a [`SchedClass`] (Linux-nice-style; higher = more CPU).
/// Foreground gets the full share; Bulk a quarter, so a background scan yields
/// ~4x more virtual runtime per real second and drifts to the back of the queue.
pub fn weight_for_class(class: SchedClass) -> u32 {
    match class {
        SchedClass::Foreground => 1024,
        SchedClass::Bulk => 256,
    }
}

/// A schedulable unit of background work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedEntity {
    /// Stable identifier of the work unit (e.g. a scan id).
    pub id: u64,
    /// Accumulated virtual runtime (service time / weight). Smaller = more
    /// deserving of the next slot.
    pub vruntime: u64,
    /// Fair-share weight (see [`weight_for_class`]); used by T1.2 to scale how
    /// fast `vruntime` advances per unit of real service.
    pub weight: u32,
}

impl SchedEntity {
    /// A unit entering the queue for the first time at `vruntime` 0.
    pub fn new(id: u64, class: SchedClass) -> Self {
        Self {
            id,
            vruntime: 0,
            weight: weight_for_class(class),
        }
    }
}

/// A single-group fair-share run queue ordered by `vruntime`.
///
/// Ordering key is `(vruntime, seq)` so equal-vruntime units are served in
/// enqueue order (FIFO among ties) rather than by `id`.
#[derive(Debug)]
pub struct RunQueue {
    tree: BTreeMap<(u64, u64), SchedEntity>,
    min_vruntime: u64,
    seq: u64,
    boost: u64,
}

impl RunQueue {
    /// A queue whose waking units may dip at most `boost` below `min_vruntime`
    /// (the bounded sleeper credit).
    pub fn new(boost: u64) -> Self {
        Self {
            tree: BTreeMap::new(),
            min_vruntime: 0,
            seq: 0,
            boost,
        }
    }

    /// Insert `entity`, clamping its `vruntime` up to at least
    /// `min_vruntime - boost` so a long-blocked unit cannot undercut running
    /// units by more than the sleeper credit. Returns the clamped vruntime.
    pub fn enqueue(&mut self, mut entity: SchedEntity) -> u64 {
        let floor = self.min_vruntime.saturating_sub(self.boost);
        entity.vruntime = entity.vruntime.max(floor);
        let key = (entity.vruntime, self.seq);
        self.seq += 1;
        self.tree.insert(key, entity);
        entity.vruntime
    }

    /// Remove and return the least-`vruntime` entity (FIFO among ties), advancing
    /// `min_vruntime` to at least the picked vruntime. `None` if empty.
    pub fn pick_next(&mut self) -> Option<SchedEntity> {
        let key = *self.tree.keys().next()?;
        let entity = self.tree.remove(&key).expect("key came from the map");
        // Monotonic: the floor only ever rises to the vruntime we just ran.
        self.min_vruntime = self.min_vruntime.max(entity.vruntime);
        Some(entity)
    }

    /// The current monotonic vruntime floor.
    pub fn min_vruntime(&self) -> u64 {
        self.min_vruntime
    }

    /// The `vruntime` of the least entity WITHOUT removing it (`None` if empty).
    /// Used by the scheduler to decide whether a running unit should yield to a
    /// more-deserving waiting one.
    pub fn peek_min_vruntime(&self) -> Option<u64> {
        self.tree.keys().next().map(|(vruntime, _seq)| *vruntime)
    }

    /// Remove the queued unit with `id`, if present; returns whether one was
    /// removed. O(n) over the *waiting* set (bounded by the admission waiter
    /// cap), used by cancellation cleanup: a scan whose consumer went away must
    /// vacate the queue so its slot is never granted to a dead id.
    pub fn remove_by_id(&mut self, id: u64) -> bool {
        let key = self
            .tree
            .iter()
            .find(|(_, entity)| entity.id == id)
            .map(|(key, _)| *key);
        match key {
            Some(key) => self.tree.remove(&key).is_some(),
            None => false,
        }
    }

    /// Number of queued units.
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ent(id: u64, vruntime: u64) -> SchedEntity {
        SchedEntity {
            id,
            vruntime,
            weight: 1024,
        }
    }

    #[test]
    fn weight_foreground_exceeds_bulk() {
        assert!(weight_for_class(SchedClass::Foreground) > weight_for_class(SchedClass::Bulk));
    }

    #[test]
    fn pick_next_returns_smallest_vruntime_fifo_among_ties() {
        let mut q = RunQueue::new(0);
        q.enqueue(ent(1, 30));
        q.enqueue(ent(2, 10));
        q.enqueue(ent(3, 10)); // ties with id 2 at vruntime 10, enqueued later
        q.enqueue(ent(4, 20));
        // 10 (id 2, first) -> 10 (id 3) -> 20 (id 4) -> 30 (id 1)
        assert_eq!(q.pick_next().unwrap().id, 2);
        assert_eq!(q.pick_next().unwrap().id, 3);
        assert_eq!(q.pick_next().unwrap().id, 4);
        assert_eq!(q.pick_next().unwrap().id, 1);
        assert!(q.pick_next().is_none());
    }

    #[test]
    fn enqueue_clamps_waking_unit_to_min_vruntime_minus_boost() {
        let mut q = RunQueue::new(5);
        q.enqueue(ent(1, 100));
        // Run id 1 so min_vruntime advances to 100.
        assert_eq!(q.pick_next().unwrap().id, 1);
        assert_eq!(q.min_vruntime(), 100);
        // A unit waking with a stale low vruntime is clamped to 100 - 5 = 95,
        // NOT allowed to re-enter at 0 and monopolize.
        let clamped = q.enqueue(ent(2, 0));
        assert_eq!(clamped, 95);
        assert_eq!(q.tree.values().next().unwrap().vruntime, 95);
    }

    #[test]
    fn enqueue_above_floor_is_not_clamped_down() {
        let mut q = RunQueue::new(5);
        q.enqueue(ent(1, 100));
        q.pick_next(); // min_vruntime -> 100
                       // A unit already ahead of the floor keeps its vruntime.
        let v = q.enqueue(ent(2, 200));
        assert_eq!(v, 200);
    }

    #[test]
    fn min_vruntime_is_monotonic_non_decreasing() {
        let mut q = RunQueue::new(0);
        let mut prev = q.min_vruntime();
        for (id, v) in [(1u64, 50u64), (2, 10), (3, 90), (4, 30)] {
            q.enqueue(ent(id, v));
            assert!(q.min_vruntime() >= prev, "floor decreased on enqueue");
            prev = q.min_vruntime();
        }
        while let Some(e) = q.pick_next() {
            assert!(q.min_vruntime() >= prev, "floor decreased on pick");
            assert!(q.min_vruntime() >= e.vruntime);
            prev = q.min_vruntime();
        }
    }

    #[test]
    fn remove_by_id_vacates_a_waiting_unit() {
        let mut q = RunQueue::new(0);
        q.enqueue(ent(1, 10));
        q.enqueue(ent(2, 20));
        q.enqueue(ent(3, 30));
        assert!(q.remove_by_id(2), "id 2 was queued");
        assert_eq!(q.len(), 2);
        assert!(!q.remove_by_id(2), "already removed");
        assert!(!q.remove_by_id(99), "never queued");
        // The survivors still pick in vruntime order.
        assert_eq!(q.pick_next().unwrap().id, 1);
        assert_eq!(q.pick_next().unwrap().id, 3);
        assert!(q.pick_next().is_none());
    }

    #[test]
    fn len_and_empty_track_membership() {
        let mut q = RunQueue::new(0);
        assert!(q.is_empty());
        q.enqueue(ent(1, 0));
        q.enqueue(ent(2, 0));
        assert_eq!(q.len(), 2);
        q.pick_next();
        assert_eq!(q.len(), 1);
        q.pick_next();
        assert!(q.is_empty());
    }
}
