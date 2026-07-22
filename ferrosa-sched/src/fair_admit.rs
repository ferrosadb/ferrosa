//! Module: Vruntime-ordered fair admission for the bounded scan pool.
//! Correctness: Correct when no more than `capacity` scans hold a slot at once,
//!   a freed slot is always granted to the least-`vruntime` waiter (weighted by
//!   class), a yielding scan re-competes and is never starved, and every slot is
//!   released on finish OR drop.
//! Last revised: 2026-07-22
//! Last changed: New module — B1.5. Replaces the FIFO tokio-`Semaphore` that
//!   `submit_scan` used for admission with the CFS-style vruntime scheduler, so
//!   the `Scheduler`/`SchedTicket`/`SchedClass` machinery is the *live* admission
//!   authority (not just tested in isolation). Weighting is real: a Foreground
//!   scan (weight 1024) advances `vruntime` 4x slower than a Bulk scan (256), so
//!   under contention it gets ~4x the slot turns.
//!
//! # Model
//!
//! `capacity` slots (`cores − reserved`). A scan `admit`s (async — a waiter is a
//! cheap async task, never a parked blocking thread, preserving the B0 property),
//! runs on a blocking thread, and every `chunk_budget` chunks calls
//! `reschedule` (from that blocking thread): its `vruntime` advances by
//! `service × BASE_WEIGHT / weight`, and if a waiting scan is now more deserving
//! it yields the slot and re-competes. The dispatcher always grants a free slot
//! to the least-`vruntime` waiter, so weighted-fair CPU share emerges.
//!
//! Deadlock-free: the mutex is held only for the O(log n) queue ops (never
//! across `.await` or the caller's work); a yielding scan releases its slot
//! before re-competing, so a slot is always available to a waiter.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::group_runqueue::{GroupId, GroupRunQueue};
use crate::runqueue::SchedEntity;
use crate::scheduler::advance_vruntime;
use crate::SchedClass;

/// Outcome of an [`admit`](FairAdmit::admit) request.
#[derive(Debug, PartialEq, Eq)]
pub enum Admitted {
    /// A slot was granted; the payload is the scan id (pass to
    /// [`reschedule`](FairAdmit::reschedule) / [`finish`](FairAdmit::finish)).
    Slot(u64),
    /// No slot was free and the waiter queue was already at its bound — the
    /// request is rejected (fail-loud backpressure) rather than piling up
    /// unboundedly. The caller must surface this, never hang or silently drop.
    Overloaded,
    /// The caller's `cancel` future fired (or the `admit` future was dropped)
    /// before a slot was granted; the queue/waiter entry has been vacated and no
    /// slot is held. A scan whose consumer went away ends here without ever
    /// occupying a slot.
    Cancelled,
}

struct State {
    /// Free slots (starts at `capacity`).
    free: usize,
    /// Scans waiting for a slot, ordered by (group `vruntime`, query `vruntime`)
    /// — the two-level hierarchical queue (B3), so scheduling is fair between
    /// tenants (groups) as well as between a tenant's queries.
    queue: GroupRunQueue,
    /// Scans currently holding a slot: id → (group, group weight, live
    /// scheduling state). The group + weight are kept so a yielding scan
    /// re-enters its own group.
    running: HashMap<u64, (GroupId, u32, SchedEntity)>,
    /// Per-waiter wakeups, keyed by scan id.
    waiters: HashMap<u64, Arc<Notify>>,
    next_id: u64,
}

/// Vruntime-ordered admission control for the scan pool.
pub struct FairAdmit {
    state: Mutex<State>,
    capacity: usize,
    /// Max scans allowed to *wait* for a slot at once. Beyond this, [`admit`]
    /// returns [`Admitted::Overloaded`] instead of enqueuing — bounded
    /// backpressure so a flood of scans cannot grow the queue without limit.
    ///
    /// [`admit`]: FairAdmit::admit
    max_waiters: usize,
}

/// Vacates a pending admission's queue/waiter entry if its future is dropped or
/// cancelled before a slot is granted — releasing a slot if the grant raced in.
/// Disarmed once the slot is successfully returned to the caller.
struct WaitGuard<'a> {
    admit: &'a FairAdmit,
    id: u64,
    armed: bool,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.admit.cancel_waiter(self.id);
        }
    }
}

impl std::fmt::Debug for FairAdmit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FairAdmit")
            .field("capacity", &self.capacity)
            .field("active", &self.active())
            .finish()
    }
}

impl FairAdmit {
    /// A pool granting at most `capacity` (≥1) concurrent slots, admitting up to
    /// `max_waiters` (≥1) scans to *wait* before rejecting further requests with
    /// [`Admitted::Overloaded`]; waking scans get `boost` sleeper credit (see
    /// [`GroupRunQueue::new`]).
    pub fn new(capacity: usize, boost: u64, max_waiters: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            state: Mutex::new(State {
                free: capacity,
                queue: GroupRunQueue::new(boost),
                running: HashMap::new(),
                waiters: HashMap::new(),
                next_id: 0,
            }),
            capacity,
            max_waiters: max_waiters.max(1),
        }
    }

    /// Max concurrent slots.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Slots currently held.
    pub fn active(&self) -> usize {
        let s = self.state.lock().expect("fair-admit poisoned");
        self.capacity - s.free
    }

    /// Admit a scan of `class`, resolving once it is granted a slot in vruntime
    /// order. Async so a waiting scan is a cheap task, not a parked blocking
    /// thread (the B0 property).
    ///
    /// Returns:
    /// - [`Admitted::Slot(id)`](Admitted::Slot) once granted;
    /// - [`Admitted::Overloaded`] immediately if no slot is free and the waiter
    ///   queue is already at `max_waiters` (bounded backpressure);
    /// - [`Admitted::Cancelled`] if `cancel` fires — or this future is dropped —
    ///   before a slot is granted. Either way the queue/waiter entry is vacated
    ///   and **no slot is held**, so a scan whose consumer went away never
    ///   occupies a slot just to discover the closed channel.
    ///
    /// Pass [`std::future::pending()`] for `cancel` when the caller has no
    /// cancellation signal. `group` (weighted by `group_weight`) is the tenant
    /// the scan belongs to — admission is fair *between* groups as well as within
    /// them (B3).
    pub async fn admit(
        &self,
        group: GroupId,
        group_weight: u32,
        class: SchedClass,
        cancel: impl Future<Output = ()>,
    ) -> Admitted {
        let (id, notify) = {
            let mut s = self.state.lock().expect("fair-admit poisoned");
            // Backpressure: no free slot and the waiter queue is full → reject
            // rather than grow the queue unboundedly. Fail-loud over silent
            // pileup; the caller surfaces this (never hangs or drops silently).
            if s.free == 0 && s.queue.len() >= self.max_waiters {
                drop(s);
                crate::record_overload_rejection();
                return Admitted::Overloaded;
            }
            let id = s.next_id;
            s.next_id += 1;
            s.queue
                .enqueue(group, group_weight, SchedEntity::new(id, class));
            let notify = Arc::new(Notify::new());
            s.waiters.insert(id, notify.clone());
            self.dispatch(&mut s);
            if s.running.contains_key(&id) {
                s.waiters.remove(&id);
                return Admitted::Slot(id);
            }
            (id, notify)
        };
        // From here the entry lives in the queue; if the future is dropped or
        // `cancel` fires before the grant, `WaitGuard` vacates it.
        let mut guard = WaitGuard {
            admit: self,
            id,
            armed: true,
        };
        tokio::pin!(cancel);
        loop {
            tokio::select! {
                // Prefer cancellation: a gone consumer should free the slot even
                // if a grant is simultaneously available.
                biased;
                _ = &mut cancel => return Admitted::Cancelled, // WaitGuard cleans up
                _ = notify.notified() => {
                    let mut s = self.state.lock().expect("fair-admit poisoned");
                    if s.running.contains_key(&id) {
                        s.waiters.remove(&id);
                        guard.armed = false;
                        return Admitted::Slot(id);
                    }
                }
            }
        }
    }

    /// Vacate a still-waiting scan `id` (cancellation cleanup): remove it from
    /// the queue and waiter map, and if the grant raced in between the caller's
    /// last check and cancellation, release the slot it was handed.
    fn cancel_waiter(&self, id: u64) {
        let mut s = self.state.lock().expect("fair-admit poisoned");
        s.waiters.remove(&id);
        if s.queue.remove_by_id(id) {
            return; // still waiting — cleanly removed
        }
        // Raced: dispatched into `running` after our last check. Give the slot
        // back so it is not leaked to a dead id.
        if s.running.remove(&id).is_some() {
            s.free += 1;
            self.dispatch(&mut s);
        }
    }

    /// Account `service` for running scan `id`, and if a waiting scan is now more
    /// deserving, yield the slot and block (on `handle`) until re-granted in
    /// vruntime order. Called from the scan's `spawn_blocking` thread, so blocking
    /// on the runtime handle is sound.
    pub fn reschedule(&self, handle: &tokio::runtime::Handle, id: u64, service: u64) {
        let notify = {
            let mut s = self.state.lock().expect("fair-admit poisoned");
            let (group, weight, mut entity) = match s.running.remove(&id) {
                Some(x) => x,
                None => return, // not currently granted — nothing to do
            };
            entity.vruntime = advance_vruntime(entity.vruntime, service, entity.weight);
            // Charge the group too, so its aggregate share reflects all its
            // queries' service (B3 group fairness).
            s.queue.charge(group, service);
            // Yield only if a strictly-more-deserving scan waits, compared
            // lexicographically (group `vruntime`, then query `vruntime`): same
            // group → the query comparison decides (within-tenant fairness);
            // different group → the group dominates (cross-tenant fairness).
            let group_vruntime = s.queue.group_vruntime(group).unwrap_or(entity.vruntime);
            let running_key = (group_vruntime, entity.vruntime);
            let should_yield = s.queue.peek_min().is_some_and(|min| min < running_key);
            if !should_yield {
                s.running.insert(id, (group, weight, entity));
                return;
            }
            // Yield: release the slot and re-compete within its group.
            s.free += 1;
            s.queue.enqueue(group, weight, entity);
            let notify = Arc::new(Notify::new());
            s.waiters.insert(id, notify.clone());
            self.dispatch(&mut s);
            if s.running.contains_key(&id) {
                s.waiters.remove(&id);
                return;
            }
            notify
        };
        handle.block_on(async move {
            loop {
                notify.notified().await;
                let mut s = self.state.lock().expect("fair-admit poisoned");
                if s.running.contains_key(&id) {
                    s.waiters.remove(&id);
                    return;
                }
            }
        });
    }

    /// Release scan `id`'s slot permanently (on finish or drop).
    pub fn finish(&self, id: u64) {
        let mut s = self.state.lock().expect("fair-admit poisoned");
        if s.running.remove(&id).is_some() {
            s.free += 1;
        }
        self.dispatch(&mut s);
    }

    /// Grant every free slot to the most-deserving waiter (least-`vruntime`
    /// query of the least-`vruntime` group) and wake it.
    fn dispatch(&self, s: &mut State) {
        while s.free > 0 {
            match s.queue.pick_next() {
                Some(picked) => {
                    let id = picked.entity.id;
                    s.free -= 1;
                    s.running
                        .insert(id, (picked.group, picked.group_weight, picked.entity));
                    if let Some(n) = s.waiters.get(&id) {
                        n.notify_one();
                    }
                }
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as Map;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use super::*;

    /// A single test tenant group (equal weight) — most tests exercise one group,
    /// so the class weighting and cancellation logic show without group effects.
    const G: GroupId = 7;
    const GW: u32 = crate::group_runqueue::DEFAULT_GROUP_WEIGHT;

    /// Unwrap a granted slot or fail the test loudly.
    fn slot(outcome: Admitted) -> u64 {
        match outcome {
            Admitted::Slot(id) => id,
            other => panic!("expected Slot, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admit_never_exceeds_capacity() {
        let admit = Arc::new(FairAdmit::new(2, 0, 32));
        let max = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let admit = admit.clone();
            let max = max.clone();
            tasks.push(tokio::spawn(async move {
                let id = slot(
                    admit
                        .admit(G, GW, SchedClass::Bulk, std::future::pending::<()>())
                        .await,
                );
                let now = admit.active();
                max.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                admit.finish(id);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(
            max.load(Ordering::SeqCst) <= 2,
            "admitted {} concurrently, capacity 2",
            max.load(Ordering::SeqCst)
        );
        assert_eq!(admit.active(), 0, "all slots released");
    }

    /// The load-bearing B1.5 property: under contention a Foreground scan gets
    /// ~4x the slot turns of a Bulk scan (weight 1024 : 256). Both scans run
    /// unbounded and stop when a SHARED budget of served chunks is exhausted, so
    /// the split of that budget reflects the scheduler's weighting (not each
    /// scan's own fixed workload).
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn foreground_gets_roughly_four_to_one_over_bulk() {
        // capacity 1 so the two scans genuinely contend for the single slot.
        let admit = Arc::new(FairAdmit::new(1, 0, 32));
        let handle = tokio::runtime::Handle::current();
        let ran: Arc<Mutex<Map<&'static str, u64>>> = Arc::new(Mutex::new(Map::new()));
        let served = Arc::new(AtomicUsize::new(0));
        const BUDGET: usize = 5000;

        let run = |class: SchedClass, tag: &'static str| {
            let admit = admit.clone();
            let handle = handle.clone();
            let ran = ran.clone();
            let served = served.clone();
            tokio::task::spawn_blocking(move || {
                let id =
                    slot(handle.block_on(admit.admit(G, GW, class, std::future::pending::<()>())));
                while served.fetch_add(1, Ordering::SeqCst) < BUDGET {
                    *ran.lock().unwrap().entry(tag).or_insert(0) += 1;
                    admit.reschedule(&handle, id, 10);
                }
                admit.finish(id);
            })
        };
        let fg = run(SchedClass::Foreground, "fg");
        let bulk = run(SchedClass::Bulk, "bulk");
        fg.await.unwrap();
        bulk.await.unwrap();

        let ran = ran.lock().unwrap();
        let (fg, bulk) = (ran["fg"] as f64, ran["bulk"] as f64);
        let ratio = fg / bulk;
        assert!(
            (3.0..=5.0).contains(&ratio),
            "expected ~4:1 Foreground:Bulk turns, got {ratio:.2} (fg={fg}, bulk={bulk})"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn equal_weight_scans_do_not_starve_each_other() {
        let admit = Arc::new(FairAdmit::new(1, 0, 32));
        let handle = tokio::runtime::Handle::current();
        let ran: Arc<Mutex<Map<u64, u64>>> = Arc::new(Mutex::new(Map::new()));
        let mut tasks = Vec::new();
        for tag in 0..3u64 {
            let admit = admit.clone();
            let handle = handle.clone();
            let ran = ran.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let id = slot(handle.block_on(admit.admit(
                    G,
                    GW,
                    SchedClass::Bulk,
                    std::future::pending::<()>(),
                )));
                for _ in 0..600 {
                    *ran.lock().unwrap().entry(tag).or_insert(0) += 1;
                    admit.reschedule(&handle, id, 10);
                }
                admit.finish(id);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let ran = ran.lock().unwrap();
        let max = *ran.values().max().unwrap();
        let min = *ran.values().min().unwrap();
        // Equal weight → near-equal service; nobody starved.
        assert!(max - min <= max / 3, "unfair: {ran:?}");
        assert_eq!(admit.active(), 0);
    }

    /// B3 — cross-tenant fairness through the LIVE admission path. Two tenants
    /// contend for one slot: group 1 runs three scans, group 2 runs one, at equal
    /// group weight. They get ~equal AGGREGATE service — the anti-gaming property
    /// holds end-to-end through `admit`/`reschedule`, not just in the queue unit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn tenants_get_equal_share_regardless_of_query_count() {
        let admit = Arc::new(FairAdmit::new(1, 0, 64));
        let handle = tokio::runtime::Handle::current();
        let served: Arc<Mutex<Map<GroupId, u64>>> = Arc::new(Mutex::new(Map::new()));
        let total = Arc::new(AtomicUsize::new(0));
        const BUDGET: usize = 6000;

        let run = |group: GroupId| {
            let admit = admit.clone();
            let handle = handle.clone();
            let served = served.clone();
            let total = total.clone();
            tokio::task::spawn_blocking(move || {
                let id = slot(handle.block_on(admit.admit(
                    group,
                    GW,
                    SchedClass::Bulk,
                    std::future::pending::<()>(),
                )));
                while total.fetch_add(1, Ordering::SeqCst) < BUDGET {
                    *served.lock().unwrap().entry(group).or_insert(0) += 1;
                    admit.reschedule(&handle, id, 10);
                }
                admit.finish(id);
            })
        };
        // Group 1: three scans (the "gaming" tenant); group 2: one scan.
        let tasks = vec![run(1), run(1), run(1), run(2)];
        for t in tasks {
            t.await.unwrap();
        }

        let served = served.lock().unwrap();
        let (a, b) = (served[&1] as f64, served[&2] as f64);
        let ratio = a / b;
        assert!(
            (0.5..=2.0).contains(&ratio),
            "tenants should get ~equal aggregate share regardless of query count: \
             group1(3 scans)={a} group2(1 scan)={b} ratio={ratio:.2}"
        );
        assert_eq!(admit.active(), 0);
    }

    /// A scan whose `cancel` fires before it is granted returns `Cancelled`,
    /// vacates the queue, and leaves NO phantom entry that would later steal a
    /// slot — the core of Ben's "dropped range stream leaks a queued admission".
    #[tokio::test]
    async fn cancel_signal_frees_slot_and_leaves_no_phantom() {
        let admit = FairAdmit::new(1, 0, 16);
        let held = slot(
            admit
                .admit(G, GW, SchedClass::Bulk, std::future::pending::<()>())
                .await,
        );
        // The single slot is held; a second scan whose cancel is already ready
        // must resolve to Cancelled rather than wait or grab a slot.
        let outcome = admit
            .admit(G, GW, SchedClass::Bulk, std::future::ready(()))
            .await;
        assert_eq!(outcome, Admitted::Cancelled);
        // Releasing the held slot finds no waiter (no phantom) → fully free.
        admit.finish(held);
        assert_eq!(admit.active(), 0, "cancelled admit leaked a slot");
    }

    /// Dropping a pending `admit` future (e.g. the detached task is aborted) must
    /// also vacate the queue entry — the belt-and-suspenders drop guard.
    #[tokio::test]
    async fn dropped_admit_future_leaves_no_phantom() {
        let admit = FairAdmit::new(1, 0, 16);
        let held = slot(
            admit
                .admit(G, GW, SchedClass::Bulk, std::future::pending::<()>())
                .await,
        );
        let mut cx = Context::from_waker(std::task::Waker::noop());
        {
            // Enqueues, then parks (no free slot). Poll once, then drop.
            let mut fut = pin!(admit.admit(G, GW, SchedClass::Bulk, std::future::pending::<()>()));
            assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        }
        admit.finish(held);
        assert_eq!(admit.active(), 0, "dropped admit leaked a slot");
    }

    /// With no free slot and the waiter queue at its bound, `admit` returns
    /// `Overloaded` (bounded backpressure) instead of enqueuing unboundedly.
    #[tokio::test]
    async fn admit_rejects_when_waiter_queue_is_full() {
        let admit = FairAdmit::new(1, 0, 2); // 1 slot, at most 2 waiters
        let held = slot(
            admit
                .admit(G, GW, SchedClass::Bulk, std::future::pending::<()>())
                .await,
        );
        let mut cx = Context::from_waker(std::task::Waker::noop());
        {
            // Fill the waiter queue (poll once each). The `pin!`ed futures live
            // to the end of this block, so they must be scoped here — dropping
            // the `Pin<&mut _>` handle would not drop the future.
            let mut w1 = pin!(admit.admit(G, GW, SchedClass::Bulk, std::future::pending::<()>()));
            let mut w2 = pin!(admit.admit(G, GW, SchedClass::Bulk, std::future::pending::<()>()));
            assert!(matches!(w1.as_mut().poll(&mut cx), Poll::Pending));
            assert!(matches!(w2.as_mut().poll(&mut cx), Poll::Pending));
            // Third request: no slot, queue full → Overloaded (resolves at once).
            let outcome = admit
                .admit(G, GW, SchedClass::Bulk, std::future::pending::<()>())
                .await;
            assert_eq!(outcome, Admitted::Overloaded);
        }
        // The two waiters dropped at the block end → queue vacated.
        admit.finish(held);
        assert_eq!(admit.active(), 0);
    }
}
