//! Process-wide self-heal metrics and the in-memory health surface.
//!
//! Metrics follow the existing `ferrosa-storage` pattern: atomic statics with
//! reader functions for Prometheus export and tests. The health surface is a
//! small mutexed snapshot the web/metrics endpoint reads to answer "does this
//! node have a data issue, and what is the controller doing about it?".

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Tables observed with >=1 corrupt-excluded SSTable this scan (gauge-like:
/// set to the current count each detection pass).
static CORRUPT_SSTABLE_TABLES: AtomicU64 = AtomicU64::new(0);
/// Total corrupt SSTable generations detected across all detection passes.
static CORRUPT_SSTABLE_DETECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Corrupt generations successfully quarantined (files moved).
static QUARANTINED_GENERATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Quarantine refusals because no healthy replica could refill (FMEA #1).
static QUARANTINE_REFUSED_NO_REPLICA_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Issues escalated to degraded after exhausting attempts (FMEA #5).
static ESCALATED_MAX_ATTEMPTS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Remediation actions executed (any kind).
static ACTIONS_EXECUTED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// 1 if the controller currently considers the node degraded, else 0.
static DEGRADED: AtomicU64 = AtomicU64::new(0);

pub(crate) fn set_corrupt_tables(n: u64) {
    CORRUPT_SSTABLE_TABLES.store(n, Ordering::Relaxed);
}
pub(crate) fn inc_corrupt_detected(n: u64) {
    CORRUPT_SSTABLE_DETECTED_TOTAL.fetch_add(n, Ordering::Relaxed);
}
pub(crate) fn inc_quarantined(n: u64) {
    QUARANTINED_GENERATIONS_TOTAL.fetch_add(n, Ordering::Relaxed);
}
pub(crate) fn inc_quarantine_refused_no_replica() {
    QUARANTINE_REFUSED_NO_REPLICA_TOTAL.fetch_add(1, Ordering::Relaxed);
}
pub(crate) fn inc_escalated_max_attempts() {
    ESCALATED_MAX_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}
pub(crate) fn inc_actions_executed() {
    ACTIONS_EXECUTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}
pub(crate) fn set_degraded(degraded: bool) {
    DEGRADED.store(u64::from(degraded), Ordering::Relaxed);
}

/// Snapshot of every self-heal metric, for Prometheus export / tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfHealMetrics {
    pub corrupt_sstable_tables: u64,
    pub corrupt_sstable_detected_total: u64,
    pub quarantined_generations_total: u64,
    pub quarantine_refused_no_replica_total: u64,
    pub escalated_max_attempts_total: u64,
    pub actions_executed_total: u64,
    pub degraded: bool,
}

/// Read all self-heal metrics atomically-ish (relaxed; counters are monotone).
pub fn self_heal_metrics() -> SelfHealMetrics {
    SelfHealMetrics {
        corrupt_sstable_tables: CORRUPT_SSTABLE_TABLES.load(Ordering::Relaxed),
        corrupt_sstable_detected_total: CORRUPT_SSTABLE_DETECTED_TOTAL.load(Ordering::Relaxed),
        quarantined_generations_total: QUARANTINED_GENERATIONS_TOTAL.load(Ordering::Relaxed),
        quarantine_refused_no_replica_total: QUARANTINE_REFUSED_NO_REPLICA_TOTAL
            .load(Ordering::Relaxed),
        escalated_max_attempts_total: ESCALATED_MAX_ATTEMPTS_TOTAL.load(Ordering::Relaxed),
        actions_executed_total: ACTIONS_EXECUTED_TOTAL.load(Ordering::Relaxed),
        degraded: DEGRADED.load(Ordering::Relaxed) != 0,
    }
}

/// **Tests only.** Reset every counter so serial tests start clean.
#[doc(hidden)]
pub fn _reset_self_heal_metrics_for_tests() {
    CORRUPT_SSTABLE_TABLES.store(0, Ordering::Relaxed);
    CORRUPT_SSTABLE_DETECTED_TOTAL.store(0, Ordering::Relaxed);
    QUARANTINED_GENERATIONS_TOTAL.store(0, Ordering::Relaxed);
    QUARANTINE_REFUSED_NO_REPLICA_TOTAL.store(0, Ordering::Relaxed);
    ESCALATED_MAX_ATTEMPTS_TOTAL.store(0, Ordering::Relaxed);
    ACTIONS_EXECUTED_TOTAL.store(0, Ordering::Relaxed);
    DEGRADED.store(0, Ordering::Relaxed);
}

/// One line on the health surface describing a current issue and the last
/// thing the controller did about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthEntry {
    pub table: String,
    pub issue: String,
    /// e.g. "detected", "quarantined gen N", "escalated: no healthy replica".
    pub status: String,
}

/// The health surface the web/metrics endpoint reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelfHealHealth {
    /// Current outstanding issues + last controller status per issue.
    pub entries: Vec<HealthEntry>,
    /// True when any issue escalated to degraded (FMEA #1 / #5).
    pub degraded: bool,
    /// Logical tick of the last completed control loop pass.
    pub last_tick: u64,
}

static HEALTH: Mutex<Option<SelfHealHealth>> = Mutex::new(None);

/// Publish the latest health surface. Called once per tick by the controller.
pub(crate) fn publish_health(health: SelfHealHealth) {
    set_degraded(health.degraded);
    if let Ok(mut guard) = HEALTH.lock() {
        *guard = Some(health);
    }
}

/// Read the current health surface (`None` until the first tick completes).
pub fn self_heal_health() -> Option<SelfHealHealth> {
    HEALTH.lock().ok().and_then(|g| g.clone())
}

/// **Tests only.** Clear the published health surface.
#[doc(hidden)]
pub fn _reset_self_heal_health_for_tests() {
    if let Ok(mut guard) = HEALTH.lock() {
        *guard = None;
    }
}
