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
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::runqueue::{RunQueue, SchedEntity};
use crate::scheduler::advance_vruntime;
use crate::SchedClass;

struct State {
    /// Free slots (starts at `capacity`).
    free: usize,
    /// Scans waiting for a slot, ordered by `vruntime`.
    queue: RunQueue,
    /// Scans currently holding a slot: id → its live scheduling state.
    running: HashMap<u64, SchedEntity>,
    /// Per-waiter wakeups, keyed by scan id.
    waiters: HashMap<u64, Arc<Notify>>,
    next_id: u64,
}

/// Vruntime-ordered admission control for the scan pool.
pub struct FairAdmit {
    state: Mutex<State>,
    capacity: usize,
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
    /// A pool granting at most `capacity` (≥1) concurrent slots; waking scans get
    /// `boost` sleeper credit (see [`RunQueue::new`]).
    pub fn new(capacity: usize, boost: u64) -> Self {
        let capacity = capacity.max(1);
        Self {
            state: Mutex::new(State {
                free: capacity,
                queue: RunQueue::new(boost),
                running: HashMap::new(),
                waiters: HashMap::new(),
                next_id: 0,
            }),
            capacity,
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

    /// Admit a scan of `class`; resolves once it is granted a slot in vruntime
    /// order. Returns the scan's id (pass to [`reschedule`](Self::reschedule) /
    /// [`finish`](Self::finish)). Async so a waiting scan is a cheap task, not a
    /// parked blocking thread.
    pub async fn admit(&self, class: SchedClass) -> u64 {
        let (id, notify) = {
            let mut s = self.state.lock().expect("fair-admit poisoned");
            let id = s.next_id;
            s.next_id += 1;
            s.queue.enqueue(SchedEntity::new(id, class));
            let notify = Arc::new(Notify::new());
            s.waiters.insert(id, notify.clone());
            self.dispatch(&mut s);
            if s.running.contains_key(&id) {
                s.waiters.remove(&id);
                return id;
            }
            (id, notify)
        };
        loop {
            notify.notified().await;
            let mut s = self.state.lock().expect("fair-admit poisoned");
            if s.running.contains_key(&id) {
                s.waiters.remove(&id);
                return id;
            }
        }
    }

    /// Account `service` for running scan `id`, and if a waiting scan is now more
    /// deserving, yield the slot and block (on `handle`) until re-granted in
    /// vruntime order. Called from the scan's `spawn_blocking` thread, so blocking
    /// on the runtime handle is sound.
    pub fn reschedule(&self, handle: &tokio::runtime::Handle, id: u64, service: u64) {
        let notify = {
            let mut s = self.state.lock().expect("fair-admit poisoned");
            let mut entity = match s.running.remove(&id) {
                Some(e) => e,
                None => return, // not currently granted — nothing to do
            };
            entity.vruntime = advance_vruntime(entity.vruntime, service, entity.weight);
            // Keep the slot unless a strictly-more-deserving scan waits.
            let should_yield =
                crate::scheduler::should_switch(entity.vruntime, s.queue.peek_min_vruntime());
            if !should_yield {
                s.running.insert(id, entity);
                return;
            }
            // Yield: release the slot and re-compete.
            s.free += 1;
            s.queue.enqueue(entity);
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

    /// Grant every free slot to the least-`vruntime` waiter and wake it.
    fn dispatch(&self, s: &mut State) {
        while s.free > 0 {
            match s.queue.pick_next() {
                Some(entity) => {
                    let id = entity.id;
                    s.free -= 1;
                    s.running.insert(id, entity);
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admit_never_exceeds_capacity() {
        let admit = Arc::new(FairAdmit::new(2, 0));
        let max = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let admit = admit.clone();
            let max = max.clone();
            tasks.push(tokio::spawn(async move {
                let id = admit.admit(SchedClass::Bulk).await;
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
        let admit = Arc::new(FairAdmit::new(1, 0));
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
                let id = handle.block_on(admit.admit(class));
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
        let admit = Arc::new(FairAdmit::new(1, 0));
        let handle = tokio::runtime::Handle::current();
        let ran: Arc<Mutex<Map<u64, u64>>> = Arc::new(Mutex::new(Map::new()));
        let mut tasks = Vec::new();
        for tag in 0..3u64 {
            let admit = admit.clone();
            let handle = handle.clone();
            let ran = ran.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let id = handle.block_on(admit.admit(SchedClass::Bulk));
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
}
