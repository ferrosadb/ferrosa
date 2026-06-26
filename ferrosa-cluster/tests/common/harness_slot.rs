//! Cross-process concurrency slot shared by every multi-node test harness.
//!
//! Each multi-node cluster test (`TestCluster`, the real-TCP `TestClusterNode`
//! harnesses, …) runs openraft / the internode transport on a 4-worker tokio
//! runtime with deliberately short timers (e.g. 50 ms heartbeat, 200–1000 ms
//! election). `cargo test` runs a binary's test functions concurrently
//! (≈ num_cpus at once), so on a high-core machine a binary full of these
//! oversubscribes the CPU and those short timers stop being serviced on time →
//! spurious elections / split votes → a cluster fails to converge on a stable
//! leader. That is a host-load artifact of the in-process/loopback harnesses
//! (production raft is isolated on a dedicated `Lane::Raft` OS thread), NOT an
//! election bug — but it makes leader-dependent tests nondeterministic.
//!
//! Every multi-node harness holds one of `K` cross-process slots while it forms
//! a cluster, bounding how many form at once across ALL test binaries to
//! `K = ceil(cores / 4)` — roughly one core's worth of raft workers — so the
//! short timers stay serviceable and election convergence is deterministic. The
//! slot is an advisory `flock` over `K` lock files: conflicts are detected at the
//! inode level across processes AND across fds within one process, so it is a
//! true counting semaphore. The lock releases when the file handle drops or the
//! process dies — no stale slots.
//!
//! # Deadlock-freedom (critical)
//!
//! A slot must be held for at most ONE cluster at a time per caller. If a single
//! test holds a slot for a cluster's whole *lifetime* and then forms a SECOND
//! cluster, it needs two slots at once — and on a low-core CI runner where
//! `K == 1` that self-deadlocks (the second `with_voters` waits forever for the
//! slot the first still holds). `TestCluster::with_voters` therefore holds the
//! slot only across the build+election window and releases it before returning
//! (see that function), so no caller ever holds more than one slot. Lifetime-held
//! slots are only safe for single-cluster tests.
//!
//! As a backstop against any future regression, acquisition has a hard deadline:
//! it panics (fail loud) rather than hanging the 6-hour CI job.

#![allow(dead_code)] // not every including binary uses every item

use std::time::Duration;

/// Hard ceiling on how long to wait for a slot before failing loud. With
/// formation-scoped slots (max one held per caller) a true wait is bounded by a
/// few cluster formations; anything approaching this is a deadlock/leak, which we
/// surface as a fast panic instead of letting the 6-hour CI job time out.
const ACQUIRE_DEADLINE: Duration = Duration::from_secs(600);

/// A held cross-process concurrency slot (see module docs). Dropping it releases
/// the slot.
pub struct HarnessSlot {
    _file: std::fs::File,
}

/// Number of concurrent multi-node cluster formations permitted across all test
/// binaries. Overridable via `FERROSA_TEST_HARNESS_SLOTS` (used by the
/// deadlock regression test to force the low-core `K == 1` condition).
fn harness_slot_count() -> usize {
    if let Ok(v) = std::env::var("FERROSA_TEST_HARNESS_SLOTS") {
        if let Ok(n) = v.parse::<usize>() {
            return n.max(1);
        }
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    // Each cluster formation runs on a 4-worker runtime; on a HIGH-core machine
    // (where `cargo test` runs many at once and oversubscribes the CPU, the
    // condition that makes the short raft timers miss) keep the aggregate near
    // one core's worth of raft workers via `ceil(cores / 4)`.
    //
    // The `.max(4)` floor is deliberate: low-core CI runners (2–4 cores) were
    // never oversubscribed — libtest already caps in-binary concurrency at
    // `num_cpus`, so they pass without any bound. Flooring K at 4 there makes the
    // slot a NO-OP (K ≥ the runner's concurrency) so we don't needlessly
    // serialize every formation and ~2.5× the CI `Test + Coverage` time. The
    // bound only bites on high-core machines, which is exactly where the
    // oversubscription flakiness lives. (Formation-scoping already guarantees
    // deadlock-freedom at any K ≥ 1; the floor is purely a CI-throughput guard.)
    cores.div_ceil(4).max(4)
}

/// Acquire one of `K` cross-process slots, holding it until the returned guard
/// drops. Waits (cooperatively) until a slot is free; `flock` auto-releases on
/// drop or process death, so a crashed peer never strands a slot. Panics if no
/// slot frees within [`ACQUIRE_DEADLINE`] — a fail-loud backstop so a harness
/// regression can never hang the CI job for hours.
pub async fn acquire_harness_slot() -> HarnessSlot {
    use fs2::FileExt;
    use std::fs::OpenOptions;

    let slots = harness_slot_count();
    let dir = std::env::temp_dir().join("ferrosa-raft-harness-slots");
    std::fs::create_dir_all(&dir).expect("create harness slot dir");

    let mut waited = Duration::ZERO;
    let poll = Duration::from_millis(25);
    loop {
        for i in 0..slots {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(dir.join(format!("slot-{i}.lock")))
                .expect("open harness slot file");
            // Non-blocking exclusive lock: succeeds only if this slot is free
            // (conflict is detected across processes and across fds in-process).
            if FileExt::try_lock_exclusive(&file).is_ok() {
                return HarnessSlot { _file: file };
            }
            // Lock not acquired — drop `file` (closes the fd, holds no lock).
        }
        assert!(
            waited < ACQUIRE_DEADLINE,
            "harness slot not acquired within {ACQUIRE_DEADLINE:?} across {slots} slots — \
             likely a slot leak or a caller holding >1 slot (see harness_slot.rs deadlock-freedom)"
        );
        tokio::time::sleep(poll).await;
        waited += poll;
    }
}
