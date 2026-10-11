//! Nightly guard: the Accord `ConflictIndex` residency cut at the live scale.
//!
//! The single assertion here is an ABSOLUTE byte bound on the projected saving at
//! N = 1,000,112 keys. An absolute bound measures the RUNNER, not the code, so —
//! exactly like `accord::perf` / `accord::perf_regression` — this test is SKIPPED
//! by name in the per-PR lane (`.github/workflows/ci.yml`, the `--skip
//! conflict_index_residency_projection_absolute` in the `Test + Coverage` job) and
//! enforced nightly (`.github/workflows/nightly-fuzz.yml`, job `perf-regression`).
//! Keep the two in sync: if this test is renamed or its bound changed, update both.
//!
//! The structural / same-run-ratio guard that DOES run per-PR is
//! `conflict_index_residency.rs`; the shared, measured harness is
//! `support/residency.rs`. The saving is a NET delta against an old-representation
//! stand-in measured in the same run, so it already accounts for the new entry
//! being wider than a bare `Vec`.

#[path = "support/residency.rs"]
mod residency;

use residency::{measure, CountingAlloc};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[test]
fn conflict_index_residency_projection_absolute() {
    let r = measure();
    println!("{}", r.report("nightly absolute guard"));

    assert!(
        r.projected_saving > 300 * 1024 * 1024,
        "the projected residency saving at N={} must exceed 300 MiB; measured \
         {:.0} MiB (saved {} B/key)",
        residency::TARGET_KEYS,
        r.projected_saving as f64 / (1024.0 * 1024.0),
        r.saved_per_key,
    );
}
