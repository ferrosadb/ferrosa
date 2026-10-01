//! Per-generation exclusion between S3 rehydration and compaction retirement.
//! Correctness: a rehydrate and a retire of one generation never overlap, and a
//! generation compaction has retired is never rehydrated back (ST-61).
//! Last revised: 2026-09-30
//! Last changed: Introduced; replaces the rehydration-only lock map.
//!
//! Rehydration downloads a generation's components into the table directory as
//! `<gen>-<component>.rehydrate.tmp` and renames each into place. Retirement
//! sweeps every `<gen>-*` entry out of that same directory. Unsynchronised, the
//! sweep moves a temp file mid-download (the promote rename then fails with
//! ENOENT) or the download resurrects components of a generation compaction had
//! just retired (a later open fails with "Data.db is missing").
//!
//! Both take the generation's slot lock. Retirement also leaves a tombstone on
//! the slot, so a rehydrate that was queued behind it sees the generation is
//! gone and refuses instead of downloading it again. Tombstones expire after
//! [`TOMBSTONE_WINDOW`]: only a reader holding a stale view of the table can
//! ask for a retired generation, and that view is replaced within seconds.

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// How long a retired generation keeps refusing rehydration.
const TOMBSTONE_WINDOW: Duration = Duration::from_secs(600);

/// Slot count above which idle slots are pruned on lookup. Bounds the map: an
/// idle slot carries no state worth keeping, a recent tombstone does.
const PRUNE_ABOVE: usize = 4096;

/// What the slot lock protects.
#[derive(Debug, Default)]
pub(crate) struct GenerationState {
    retired_at: Option<Instant>,
}

impl GenerationState {
    /// Records that compaction retired the generation. Call only once its
    /// components are gone.
    pub(crate) fn mark_retired(&mut self) {
        self.retired_at = Some(Instant::now());
    }

    /// True while a recent retirement forbids rehydrating the generation.
    pub(crate) fn is_retired(&self) -> bool {
        self.retired_at
            .is_some_and(|at| at.elapsed() < TOMBSTONE_WINDOW)
    }
}

/// One generation's lock.
#[derive(Debug, Default)]
pub(crate) struct GenerationSlot {
    state: Mutex<GenerationState>,
}

impl GenerationSlot {
    /// Blocks until no other rehydrate or retire of this generation runs.
    /// The state holds a timestamp only, so a poisoned lock is still valid.
    pub(crate) fn lock(&self) -> MutexGuard<'_, GenerationState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_prunable(&self) -> bool {
        match self.state.try_lock() {
            Ok(state) => !state.is_retired(),
            Err(std::sync::TryLockError::Poisoned(p)) => !p.into_inner().is_retired(),
            Err(std::sync::TryLockError::WouldBlock) => false,
        }
    }
}

static SLOTS: LazyLock<DashMap<PathBuf, Arc<GenerationSlot>>> = LazyLock::new(DashMap::new);

/// The slot for generation `gen` of the table directory `table_dir`.
pub(crate) fn slot(table_dir: &Path, gen: &str) -> Arc<GenerationSlot> {
    if SLOTS.len() > PRUNE_ABOVE {
        SLOTS.retain(|_, slot| Arc::strong_count(slot) > 1 || !slot.is_prunable());
    }
    Arc::clone(
        SLOTS
            .entry(table_dir.join(gen))
            .or_insert_with(|| Arc::new(GenerationSlot::default()))
            .value(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retirement_tombstones_only_its_own_generation() {
        let dir = tempfile::tempdir().unwrap();
        slot(dir.path(), "7").lock().mark_retired();
        assert!(slot(dir.path(), "7").lock().is_retired());
        assert!(!slot(dir.path(), "8").lock().is_retired());
    }

    #[test]
    fn a_slot_is_shared_by_every_lookup_of_the_same_generation() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Arc::ptr_eq(&slot(dir.path(), "7"), &slot(dir.path(), "7")));
    }
}
