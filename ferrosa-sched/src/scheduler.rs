//! Module: Virtual-runtime accounting for the fair-share scan scheduler.
//! Correctness: Correct when `advance_vruntime` charges service inversely to
//!   weight (saturating) and `should_switch` yields exactly when a waiting scan
//!   is strictly more deserving.
//! Last revised: 2026-07-22
//! Last changed: Reduced to the pure vruntime primitives. The standalone
//!   `Scheduler` / `SchedTicket` abstraction (B1 T1.2 core) was superseded by
//!   [`crate::fair_admit::FairAdmit`] — the live admission authority that couples
//!   these primitives with the pool's slot accounting — and removed to avoid dead
//!   code. `FairAdmit` uses `advance_vruntime` + `should_switch` directly.
//!
//! # Accounting
//!
//! A scan's `vruntime` advances by `service × BASE_WEIGHT / weight`. A
//! full-weight (Foreground, 1024) scan advances one-for-one with real service; a
//! quarter-weight (Bulk, 256) scan advances 4x faster, so it drifts behind and
//! cedes the pool to interactive work. Because admission always runs the
//! smallest-`vruntime` scan, equal-weight scans converge to equal service and
//! unequal-weight scans to service proportional to weight.

/// Reference fair-share weight (a Foreground scan). Service for a scan of this
/// weight advances `vruntime` one-for-one.
pub const BASE_WEIGHT: u64 = 1024;

/// Advance `vruntime` by `service` charged inversely to `weight`. Pure.
///
/// `delta = service × BASE_WEIGHT / weight`. Saturating so a pathological
/// service value can never wrap the counter.
pub fn advance_vruntime(vruntime: u64, service: u64, weight: u32) -> u64 {
    let weight = u64::from(weight.max(1));
    let delta = service.saturating_mul(BASE_WEIGHT) / weight;
    vruntime.saturating_add(delta)
}

/// Whether a scan at `running_vruntime` should cede its slot to the smallest
/// waiting scan. Pure. Yields only when a waiter is STRICTLY more deserving, so
/// equal-vruntime scans don't thrash back and forth.
pub fn should_switch(running_vruntime: u64, min_waiting_vruntime: Option<u64>) -> bool {
    match min_waiting_vruntime {
        Some(waiting) => waiting < running_vruntime,
        None => false,
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
}
