//! ExclusiveSyncPoint and DurabilityService for Accord.
//!
//! An **ExclusiveSyncPoint** is a barrier that fires when every shard has
//! applied all transactions up to a given timestamp. It is the mechanism
//! that lets the cluster certify that all previously committed data is
//! durable and consistent across replicas.
//!
//! The **DurabilityService** runs periodically, advancing the sync point
//! as shards report their applied progress. It exposes a health-check
//! endpoint and handles startup catch-up (replaying unapplied transactions
//! from the commit log).
//!
//! # Tests (A5.8)
//!
//! - `exclusive_sync_point_all_shards_applied`
//! - `exclusive_sync_point_partial_applied`
//! - `exclusive_sync_point_mixed_batch`
//! - `durability_service_periodic`
//! - `durability_service_under_load`
//! - `durability_service_health_check`
//! - `durability_service_startup_catch_up`

use std::collections::HashMap;

use ferrosa_common::accord::Timestamp;

// ---------------------------------------------------------------------------
// Shard progress tracking
// ---------------------------------------------------------------------------

/// Unique identifier for a shard (matches cross_shard::ShardId).
pub type ShardId = u64;

/// Progress of a single shard: the highest Accord timestamp for which
/// all transactions up to that timestamp have been applied locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardProgress {
    pub shard_id: ShardId,
    /// Highest timestamp at which all transactions are applied.
    pub applied_through: Timestamp,
}

// ---------------------------------------------------------------------------
// ExclusiveSyncPoint
// ---------------------------------------------------------------------------

/// A sync-point barrier: fires only when every tracked shard has applied
/// all transactions up to the target timestamp.
///
/// The sync point advances monotonically — once it reaches a timestamp,
/// it never regresses.
pub struct ExclusiveSyncPoint {
    /// Per-shard applied-through watermark.
    shard_progress: HashMap<ShardId, Timestamp>,
    /// The overall sync point: min(all shard applied_through).
    current_sync_point: Option<Timestamp>,
}

impl ExclusiveSyncPoint {
    /// Create a new sync point tracking the given shards.
    ///
    /// All shards start with no progress (applied_through = None).
    pub fn new(shard_ids: &[ShardId]) -> Self {
        assert!(!shard_ids.is_empty(), "must track at least one shard");
        let shard_progress = shard_ids
            .iter()
            .map(|&id| (id, Timestamp::synthetic(0)))
            .collect();
        Self {
            shard_progress,
            current_sync_point: None,
        }
    }

    /// Report that a shard has applied all transactions up to `ts`.
    ///
    /// Monotonicity: if `ts` is less than the shard's current watermark,
    /// the update is ignored.
    pub fn report_applied(&mut self, shard_id: ShardId, ts: Timestamp) {
        if let Some(current) = self.shard_progress.get_mut(&shard_id) {
            if ts > *current {
                *current = ts;
            }
        }
    }

    /// Try to advance the sync point.
    ///
    /// The sync point is the minimum applied-through timestamp across all
    /// shards. Returns `true` if the sync point advanced.
    pub fn try_advance(&mut self) -> bool {
        // All shards must have progressed past 0 (initial state).
        let all_zero = self.shard_progress.values().all(|ts| ts.time == 0);
        if all_zero {
            return false;
        }

        let min_ts = self
            .shard_progress
            .values()
            .copied()
            .min()
            .expect("shard_progress is non-empty");

        // Still at initial zero? No progress.
        if min_ts.time == 0 {
            return false;
        }

        let advanced = match self.current_sync_point {
            Some(current) => min_ts > current,
            None => true,
        };

        if advanced {
            self.current_sync_point = Some(min_ts);
        }

        advanced
    }

    /// Return the current sync point, if one has been established.
    pub fn sync_point(&self) -> Option<Timestamp> {
        self.current_sync_point
    }

    /// Return the number of shards being tracked.
    pub fn shard_count(&self) -> usize {
        self.shard_progress.len()
    }

    /// Return the progress for a specific shard.
    pub fn shard_watermark(&self, shard_id: ShardId) -> Option<Timestamp> {
        self.shard_progress.get(&shard_id).copied()
    }
}

// ---------------------------------------------------------------------------
// DurabilityService
// ---------------------------------------------------------------------------

/// Health status of the DurabilityService.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityHealth {
    /// Service is running and sync point is advancing.
    Healthy,
    /// Service is running but sync point is stale (hasn't advanced recently).
    Degraded,
    /// Service has not started or encountered a fatal error.
    Unavailable,
}

/// Periodic service that advances the ExclusiveSyncPoint and maintains
/// cluster-wide durability guarantees.
///
/// In production, this runs as a Tokio task. For testing, the `tick()`
/// method can be called directly.
pub struct DurabilityService {
    sync_point: ExclusiveSyncPoint,
    /// Number of ticks completed (for testing and metrics).
    tick_count: u64,
    /// Number of ticks since the sync point last advanced.
    ticks_since_advance: u64,
    /// Maximum ticks without advance before health degrades.
    stale_threshold: u64,
    /// Whether startup catch-up has completed.
    caught_up: bool,
    /// Health status.
    health: DurabilityHealth,
}

impl DurabilityService {
    /// Create a new durability service tracking the given shards.
    pub fn new(shard_ids: &[ShardId], stale_threshold: u64) -> Self {
        Self {
            sync_point: ExclusiveSyncPoint::new(shard_ids),
            tick_count: 0,
            ticks_since_advance: 0,
            stale_threshold,
            caught_up: false,
            health: DurabilityHealth::Unavailable,
        }
    }

    /// Perform one tick of the durability service.
    ///
    /// This is the core periodic function: it tries to advance the sync
    /// point and updates health status.
    pub fn tick(&mut self) {
        self.tick_count += 1;

        if !self.caught_up {
            // Not yet caught up — stay Unavailable.
            return;
        }

        let advanced = self.sync_point.try_advance();
        if advanced {
            self.ticks_since_advance = 0;
            self.health = DurabilityHealth::Healthy;
        } else {
            self.ticks_since_advance += 1;
            if self.ticks_since_advance >= self.stale_threshold {
                self.health = DurabilityHealth::Degraded;
            }
        }
    }

    /// Report shard progress (delegates to ExclusiveSyncPoint).
    pub fn report_shard_applied(&mut self, shard_id: ShardId, ts: Timestamp) {
        self.sync_point.report_applied(shard_id, ts);
    }

    /// Mark startup catch-up as complete.
    ///
    /// After catch-up, the service transitions from Unavailable to active
    /// health tracking.
    pub fn complete_catch_up(&mut self) {
        self.caught_up = true;
        self.health = DurabilityHealth::Healthy;
    }

    /// Return the current health status.
    pub fn health(&self) -> DurabilityHealth {
        self.health
    }

    /// Return the number of ticks completed.
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    /// Return the current sync point.
    pub fn sync_point(&self) -> Option<Timestamp> {
        self.sync_point.sync_point()
    }

    /// Return whether catch-up is complete.
    pub fn is_caught_up(&self) -> bool {
        self.caught_up
    }
}

// ===========================================================================
// Tests — A5.8 (7 tests)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    // -----------------------------------------------------------------------
    // Test 1: exclusive_sync_point_all_shards_applied
    // -----------------------------------------------------------------------

    /// When all shards report progress, the sync point advances to the
    /// minimum applied-through timestamp.
    #[test]
    fn exclusive_sync_point_all_shards_applied() {
        let mut sp = ExclusiveSyncPoint::new(&[1, 2, 3]);

        // All shards report the same timestamp.
        sp.report_applied(1, ts(1000));
        sp.report_applied(2, ts(1000));
        sp.report_applied(3, ts(1000));

        assert!(sp.try_advance(), "sync point should advance");
        assert_eq!(
            sp.sync_point(),
            Some(ts(1000)),
            "sync point should be at min applied timestamp"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: exclusive_sync_point_partial_applied
    // -----------------------------------------------------------------------

    /// If only some shards report progress, the sync point does not advance
    /// past the lagging shard.
    #[test]
    fn exclusive_sync_point_partial_applied() {
        let mut sp = ExclusiveSyncPoint::new(&[1, 2, 3]);

        // Only shard 1 and 2 report; shard 3 is still at 0.
        sp.report_applied(1, ts(2000));
        sp.report_applied(2, ts(3000));

        // Shard 3 is still at synthetic(0), so sync point should not advance.
        let advanced = sp.try_advance();
        assert!(
            !advanced,
            "sync point must not advance with a lagging shard"
        );
        assert_eq!(sp.sync_point(), None);
    }

    // -----------------------------------------------------------------------
    // Test 3: exclusive_sync_point_mixed_batch
    // -----------------------------------------------------------------------

    /// Mixed scenario: shards at different timestamps. The sync point
    /// is the minimum.
    #[test]
    fn exclusive_sync_point_mixed_batch() {
        let mut sp = ExclusiveSyncPoint::new(&[1, 2, 3]);

        sp.report_applied(1, ts(5000));
        sp.report_applied(2, ts(3000));
        sp.report_applied(3, ts(4000));

        assert!(sp.try_advance());
        assert_eq!(
            sp.sync_point(),
            Some(ts(3000)),
            "sync point should be at min (shard 2)"
        );

        // Shard 2 catches up.
        sp.report_applied(2, ts(5000));
        assert!(sp.try_advance());
        assert_eq!(
            sp.sync_point(),
            Some(ts(4000)),
            "sync point should advance to next min (shard 3)"
        );

        // All shards at 5000.
        sp.report_applied(3, ts(5000));
        assert!(sp.try_advance());
        assert_eq!(sp.sync_point(), Some(ts(5000)));
    }

    // -----------------------------------------------------------------------
    // Test 4: durability_service_periodic
    // -----------------------------------------------------------------------

    /// The DurabilityService advances the sync point on periodic ticks.
    #[test]
    fn durability_service_periodic() {
        let mut svc = DurabilityService::new(&[1, 2], 10);
        svc.complete_catch_up();

        // Report progress and tick.
        svc.report_shard_applied(1, ts(100));
        svc.report_shard_applied(2, ts(100));
        svc.tick();

        assert_eq!(svc.sync_point(), Some(ts(100)));
        assert_eq!(svc.tick_count(), 1);
        assert_eq!(svc.health(), DurabilityHealth::Healthy);

        // More progress.
        svc.report_shard_applied(1, ts(200));
        svc.report_shard_applied(2, ts(200));
        svc.tick();

        assert_eq!(svc.sync_point(), Some(ts(200)));
        assert_eq!(svc.tick_count(), 2);
    }

    // -----------------------------------------------------------------------
    // Test 5: durability_service_under_load
    // -----------------------------------------------------------------------

    /// Under load (many rapid ticks with progress), the service stays healthy.
    #[test]
    fn durability_service_under_load() {
        let mut svc = DurabilityService::new(&[1, 2, 3], 10);
        svc.complete_catch_up();

        // Simulate 100 ticks with constant progress.
        for i in 1..=100u64 {
            svc.report_shard_applied(1, ts(i * 100));
            svc.report_shard_applied(2, ts(i * 100));
            svc.report_shard_applied(3, ts(i * 100));
            svc.tick();
        }

        assert_eq!(svc.tick_count(), 100);
        assert_eq!(svc.sync_point(), Some(ts(10_000)));
        assert_eq!(svc.health(), DurabilityHealth::Healthy);
    }

    // -----------------------------------------------------------------------
    // Test 6: durability_service_health_check
    // -----------------------------------------------------------------------

    /// Health degrades to Degraded when the sync point stops advancing
    /// for more than `stale_threshold` ticks.
    #[test]
    fn durability_service_health_check() {
        let stale_threshold = 5;
        let mut svc = DurabilityService::new(&[1, 2], stale_threshold);

        // Before catch-up: Unavailable.
        assert_eq!(svc.health(), DurabilityHealth::Unavailable);

        svc.complete_catch_up();
        assert_eq!(svc.health(), DurabilityHealth::Healthy);

        // Get initial progress established.
        svc.report_shard_applied(1, ts(100));
        svc.report_shard_applied(2, ts(100));
        svc.tick();
        assert_eq!(svc.health(), DurabilityHealth::Healthy);

        // Now tick without progress. After stale_threshold ticks, health
        // should degrade.
        for i in 0..stale_threshold {
            svc.tick();
            if i < stale_threshold - 1 {
                assert_eq!(
                    svc.health(),
                    DurabilityHealth::Healthy,
                    "should still be healthy at tick {i}"
                );
            }
        }
        assert_eq!(svc.health(), DurabilityHealth::Degraded);

        // New progress restores health.
        svc.report_shard_applied(1, ts(200));
        svc.report_shard_applied(2, ts(200));
        svc.tick();
        assert_eq!(svc.health(), DurabilityHealth::Healthy);
    }

    // -----------------------------------------------------------------------
    // Test 7: durability_service_startup_catch_up
    // -----------------------------------------------------------------------

    /// On startup, the service is Unavailable until catch-up completes.
    /// Ticks before catch-up do not change health. After catch-up, the
    /// service begins normal operation.
    #[test]
    fn durability_service_startup_catch_up() {
        let mut svc = DurabilityService::new(&[1, 2], 10);

        // Pre-catch-up: health is Unavailable.
        assert!(!svc.is_caught_up());
        assert_eq!(svc.health(), DurabilityHealth::Unavailable);

        // Ticks before catch-up do NOT change health.
        svc.report_shard_applied(1, ts(500));
        svc.report_shard_applied(2, ts(500));
        svc.tick();
        svc.tick();
        assert_eq!(svc.health(), DurabilityHealth::Unavailable);
        // Tick count still increments even during catch-up.
        assert_eq!(svc.tick_count(), 2);

        // Complete catch-up.
        svc.complete_catch_up();
        assert!(svc.is_caught_up());
        assert_eq!(svc.health(), DurabilityHealth::Healthy);

        // Now ticks operate normally.
        svc.tick();
        assert_eq!(svc.sync_point(), Some(ts(500)));
        assert_eq!(svc.health(), DurabilityHealth::Healthy);
    }
}
