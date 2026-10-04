//! B1 T1.3 + T1.5 + T1.7 source-inspection guards for the scan/scheduler seam.
//!
//! * **T1.3** — every `submit_scan` scan producer must call `slot.tick()` in its
//!   page loop, or a long scan holds its pool slot for the whole scan and
//!   reintroduces the monopolization B1 fixes.
//! * **T1.5** — the scheduler pool must be used *only* by the `range_iter*` scan
//!   producers, so a `PartitionKeyLookup` point read never touches the scheduler
//!   (zero overhead, never queued behind a scan).
//! * **T1.7** — a producer must hold **no lock** in its run. A run that is told
//!   to yield returns and is re-admitted later, so a storage/index lock held in
//!   it would be held across a wait for the slot (FM-3/FM-7), mirroring the
//!   Accord `handlers.rs` "no lock across `.await`".
//! * **ST-84** — a producer never waits on its thread: one producer shape,
//!   `RangeScan::run`, which pauses (returns) on a full channel.
//!
//! Static analysis can't see these call/no-call invariants, so this test greps
//! the source — the same "guard the invariant at the source" pattern as the
//! viz-drain `truncations.push` check. It fails the build (not production) if a
//! future producer forgets to yield or holds a lock across the yield.

use std::fs;

/// Every range-scan producer is `RangeScan::run` (whole partitions or
/// fragments), run and re-run after each pause or yield by
/// `spawn_resumable_range_scan`.
fn producer_bodies(src: &str) -> Vec<&str> {
    vec![range_scan_run_body(src)]
}

/// The body of `RangeScan::run`: from its declaration to the next item.
fn range_scan_run_body(src: &str) -> &str {
    let impl_at = src
        .find("impl<F: FlushTarget> PausableScan for RangeScan<F>")
        .expect("RangeScan's PausableScan impl must exist");
    let rest = &src[impl_at..];
    let run_at = rest.find("fn run(").expect("RangeScan::run must exist");
    let body = &rest[run_at..];
    let end = body.find("\nfn ").unwrap_or(body.len());
    &body[..end]
}

/// Functions that may call `global_pool()`: the one helper that routes a range
/// scan through the scheduler.
const POOL_HELPERS: [&str; 1] = ["spawn_resumable_range_scan"];

/// `store.rs` up to its unit-test module: the guards are about production
/// call paths, and the tests drive the helpers directly.
fn store_src() -> String {
    let src = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/store.rs"))
        .expect("read store.rs source");
    let tests_at = src
        .find("#[cfg(test)]\nmod tests {")
        .expect("store.rs keeps its unit tests in `mod tests`");
    src[..tests_at].to_string()
}

#[test]
fn every_submit_scan_producer_calls_slot_tick() {
    let src = store_src();
    let bodies = producer_bodies(&src);
    assert_eq!(bodies.len(), 1, "one producer shape: RangeScan::run");
    for (n, body) in bodies.iter().enumerate() {
        assert!(
            body.contains("slot.tick()"),
            "range-scan producer #{n} does not call slot.tick() in its page loop — a long \
             scan would monopolize the bounded pool and reintroduce the starvation B1 fixes \
             (T1.3 / FM-2)"
        );
    }
}

#[test]
fn scheduler_pool_is_reached_only_through_the_range_scan_helper() {
    // B1 T1.5 / FM-6 — interactive point-read bypass. Three invariants keep the
    // scheduler off the point-read path:
    //   (a) `global_pool()` is called ONLY inside `spawn_resumable_range_scan`;
    //   (b) `spawn_resumable_range_scan(...)` is called only from
    //       `range_scan_stream`; and
    //   (c) `range_scan_stream` and `whole_partition_range_scan` are called only
    //       from `range_iter*` (or each other).
    // Together they guarantee the point-read methods (`read` /
    // `read_limited_rows` / `read_clustering_row`) never touch the scheduler, so a
    // `PartitionKeyLookup` has zero scheduler calls.
    let src = store_src();

    let mut pool_calls = 0usize;
    for (idx, _) in src.match_indices("global_pool()") {
        let name = enclosing_fn_name(&src, idx);
        assert!(
            POOL_HELPERS.contains(&name.as_str()),
            "global_pool() is called from `{name}` — the scheduler pool must be reached only \
             through {POOL_HELPERS:?}. A point read must have zero scheduler calls \
             (T1.5 / FM-6)."
        );
        pool_calls += 1;
    }
    assert!(
        pool_calls >= 1,
        "expected {POOL_HELPERS:?} to call global_pool()"
    );

    let mut call_sites = 0usize;
    for (idx, _) in src.match_indices("spawn_resumable_range_scan(tx") {
        let name = enclosing_fn_name(&src, idx);
        assert_eq!(
            name, "range_scan_stream",
            "spawn_resumable_range_scan is called from `{name}` (T1.5 / FM-6)"
        );
        call_sites += 1;
    }
    for pattern in [
        "self.range_scan_stream(",
        "self.whole_partition_range_scan(",
    ] {
        for (idx, _) in src.match_indices(pattern) {
            let name = enclosing_fn_name(&src, idx);
            assert!(
                name.contains("range_iter") || name == "whole_partition_range_scan",
                "{pattern} is called from `{name}` — only range_iter* scan producers may \
                 route work through the scheduler pool (T1.5 / FM-6)."
            );
            call_sites += 1;
        }
    }
    assert!(
        call_sites >= 5,
        "expected >= 5 range scan producer call sites, found {call_sites}"
    );
}

/// A scan whose consumer stopped reading (a suspended PG portal, a client
/// that left its socket full) must give back its thread, not only its slot,
/// and must never wait for room on it: the consumer may need a thread to make
/// room (ST-84). So `RangeScan::run` sends only through
/// `deliver_or_pause`/`deliver_failure`, and nothing in it blocks on the
/// channel.
#[test]
fn the_producer_pauses_instead_of_waiting() {
    let src = store_src();
    let run = range_scan_run_body(&src);
    assert!(
        run.contains("deliver_or_pause("),
        "RangeScan::run must send through deliver_or_pause"
    );
    for waiting in ["blocking_send", "block_on", ".park(", "recv_timeout"] {
        assert!(
            !run.contains(waiting),
            "RangeScan::run calls `{waiting}`, which waits on the producer's thread — \
             pause with deliver_or_pause/deliver_failure instead"
        );
    }
}

/// Name of the `fn` enclosing byte offset `at` (nearest preceding declaration).
fn enclosing_fn_name(src: &str, at: usize) -> String {
    let before = &src[..at];
    let fn_pos = before.rfind("fn ").expect("call must be inside a fn");
    before[fn_pos + "fn ".len()..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

#[test]
fn no_lock_held_across_the_cooperative_yield() {
    let src = store_src();
    for (n, body) in producer_bodies(&src).iter().enumerate() {
        // `.lock()` is the clear mutex-guard signal. A run that yields is
        // re-admitted later, so a guard taken in it risks deadlock (T1.7).
        // The producers deliberately use arc-swap `load_full()` (owned Arcs), so
        // no guard is held; this fails if a future edit introduces one.
        assert!(
            !body.contains(".lock()"),
            "range-scan producer #{n} acquires a `.lock()` guard in its run — a run that \
             yields is re-admitted later, so the lock would be held across a wait for the \
             slot (T1.7 / FM-3/FM-7). Load shared state via arc-swap (`load_full()`) instead."
        );
    }
}
