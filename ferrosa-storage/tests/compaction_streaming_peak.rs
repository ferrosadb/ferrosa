//! Behavioral guard for the real executor's partition and large-cell residency.
//! Correctness: exact values, tombstones and BTI lookups plus measured heap bounds.
//! Last revised: 2026-10-23
//! Last changed: Wired the public-executor allocation REDs into the default suite.
//!
//! The same harness drives the Fly before/after probe. One test owns this process's
//! counting allocator, including compaction worker threads. Fixtures and readback
//! assertions stay outside the measured submission/completion window.

#[path = "../examples/compaction_allocation_probe.rs"]
mod probe;

/// These budgets come from public-API REDs, not missing new cursor symbols:
/// baseline 2048x64B peaked at19,748,931 bytes (>16MiB), while a one-row four-way
/// compaction with two8MiB cells peaked at88,347,248 bytes (>32MiB). Reverting the
/// streaming implementation preserves compilation and exact values, but violates
/// these resource assertions. Timing is reported, never used as a flaky CI gate.
#[test]
fn compaction_streams_partitions_and_large_values_with_bounded_heap() {
    probe::run(probe::Config {
        rows: 2048,
        partitions: 1,
        fanin: 4,
        bytes: 64,
        repetitions: 1,
        max_peak_bytes: Some(16 * 1024 * 1024),
        max_alloc_events: Some(10_000),
        ..probe::Config::default()
    });
    probe::run(probe::Config {
        rows: 1,
        partitions: 1,
        fanin: 4,
        bytes: 8 * 1024 * 1024,
        repetitions: 1,
        max_peak_bytes: Some(32 * 1024 * 1024),
        ..probe::Config::default()
    });
}
