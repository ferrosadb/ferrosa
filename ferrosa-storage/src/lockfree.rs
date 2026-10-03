//! Module: Bounded compare-and-swap updates of `ArcSwap`-published state.
//! Correctness: Correct when every update is derived from the value it
//! replaces (no lost update), a contended update retries a bounded number of
//! times and then fails loudly, and readers never block.
//! Last revised: 2026-10-03
//! Last changed: Added for the lock-free table registry (t_d938e6ae).
//!
//! Shared engine state that used to sit behind a `RwLock` is published as an
//! immutable value in an `ArcSwap`. Readers `load()` it and never wait. A
//! writer derives the next value from the current one and installs it with a
//! compare-and-swap, retrying if another writer got there first. `ArcSwap::rcu`
//! does the same but retries without bound; [`update`] caps the retries (JPL
//! rule 2) and reports the cap as an error instead of spinning forever.

use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;

/// How many times [`update`] re-derives its value after losing a race before
/// it gives up. Updates of this kind are rare (DDL, pin bookkeeping), so a
/// thousand consecutive losses means something is spinning, not contending.
pub(crate) const MAX_CAS_ATTEMPTS: usize = 1_000;

/// Atomically replace `cell`'s value with one derived from it.
///
/// `derive` sees the current value and returns `(next, result)`: `None` for
/// `next` leaves the cell unchanged. If another writer replaces the value
/// between the load and the swap, `derive` runs again on the newer value, so
/// it must be a pure function of its input. Returns the `result` of the
/// attempt that took effect.
///
/// # Errors
///
/// Fails after [`MAX_CAS_ATTEMPTS`] lost races, naming `what` so the log says
/// which state was contended. Nothing was changed when it fails.
pub(crate) fn update<T, R>(
    cell: &ArcSwap<T>,
    what: &'static str,
    mut derive: impl FnMut(&T) -> (Option<T>, R),
) -> ferrosa_common::Result<R> {
    for _ in 0..MAX_CAS_ATTEMPTS {
        let current = cell.load_full();
        let (next, result) = derive(&current);
        let Some(next) = next else {
            return Ok(result);
        };
        let previous = cell.compare_and_swap(&current, Arc::new(next));
        if Arc::ptr_eq(&previous, &current) {
            return Ok(result);
        }
    }
    tracing::error!(
        what,
        attempts = MAX_CAS_ATTEMPTS,
        "lock-free update lost every compare-and-swap race; nothing was changed"
    );
    Err(ferrosa_common::Error::InvalidData(format!(
        "{what}: lost {MAX_CAS_ATTEMPTS} consecutive compare-and-swap races"
    )))
}

/// A set of names published as one immutable `HashSet` through an
/// `ArcSwap`: membership checks never wait, and every change is a
/// compare-and-swap derived from the set it replaces, so concurrent changes
/// never lose one another.
#[derive(Debug)]
pub(crate) struct SharedSet {
    /// Named in the error when a change loses every race (see [`update`]).
    what: &'static str,
    items: ArcSwap<HashSet<String>>,
}

impl SharedSet {
    pub(crate) fn new(what: &'static str) -> Self {
        Self {
            what,
            items: ArcSwap::from_pointee(HashSet::new()),
        }
    }

    /// Add `item`; whether it was absent.
    pub(crate) fn insert(&self, item: &str) -> ferrosa_common::Result<bool> {
        update(&self.items, self.what, |items| {
            if items.contains(item) {
                return (None, false);
            }
            let mut next = items.clone();
            next.insert(item.to_string());
            (Some(next), true)
        })
    }

    /// Remove `item`; whether it was present.
    pub(crate) fn remove(&self, item: &str) -> ferrosa_common::Result<bool> {
        update(&self.items, self.what, |items| {
            if !items.contains(item) {
                return (None, false);
            }
            let mut next = items.clone();
            next.remove(item);
            (Some(next), true)
        })
    }

    pub(crate) fn contains(&self, item: &str) -> bool {
        self.items.load().contains(item)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.load().is_empty()
    }

    /// Every member, as of one snapshot.
    pub(crate) fn to_vec(&self) -> Vec<String> {
        self.items.load().iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_update_derives_from_the_current_value() {
        let cell = ArcSwap::from_pointee(1_u32);
        let seen = update(&cell, "test", |v| (Some(v + 1), *v)).unwrap();
        assert_eq!(seen, 1);
        assert_eq!(**cell.load(), 2);
    }

    #[test]
    fn declining_to_change_leaves_the_value() {
        let cell = ArcSwap::from_pointee(7_u32);
        let result = update(&cell, "test", |_| (None, "kept")).unwrap();
        assert_eq!(result, "kept");
        assert_eq!(**cell.load(), 7);
    }

    #[test]
    fn concurrent_increments_lose_no_update() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;
        let cell = Arc::new(ArcSwap::from_pointee(0_usize));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let cell = Arc::clone(&cell);
                std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        update(&cell, "counter", |v| (Some(v + 1), ())).unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(**cell.load(), THREADS * PER_THREAD);
    }

    /// The quarantine sets were `RwLock<HashSet>`; their replacement must not
    /// lose a concurrent insert or remove (t_d938e6ae).
    #[test]
    fn concurrent_set_changes_lose_no_update() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 100;
        let set = Arc::new(SharedSet::new("test set"));
        // Every thread inserts its own names, then removes the odd ones.
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let set = Arc::clone(&set);
                std::thread::spawn(move || {
                    (0..PER_THREAD).for_each(|i| {
                        assert!(set.insert(&format!("{t}-{i}")).unwrap());
                    });
                    (1..PER_THREAD).step_by(2).for_each(|i| {
                        assert!(set.remove(&format!("{t}-{i}")).unwrap());
                    });
                })
            })
            .collect();
        handles
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
        let mut members = set.to_vec();
        members.sort();
        let mut expected: Vec<String> = (0..THREADS)
            .flat_map(|t| (0..PER_THREAD).step_by(2).map(move |i| format!("{t}-{i}")))
            .collect();
        expected.sort();
        assert_eq!(members, expected);
        assert!(
            !set.insert("0-0").unwrap(),
            "a member is not inserted twice"
        );
        assert!(!set.remove("0-1").unwrap(), "a non-member is not removed");
    }
}
