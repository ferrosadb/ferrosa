//! B1 T1.3 + T1.5 + T1.7 source-inspection guards for the scan/scheduler seam.
//!
//! * **T1.3** — every `submit_scan` scan producer must call `slot.tick()` in its
//!   page loop, or a long scan holds its pool slot for the whole scan and
//!   reintroduces the monopolization B1 fixes.
//! * **T1.5** — the scheduler pool must be used *only* by the `range_iter*` scan
//!   producers, so a `PartitionKeyLookup` point read never touches the scheduler
//!   (zero overhead, never queued behind a scan).
//! * **T1.7** — a producer must hold **no lock across `slot.tick()`**. `tick()`
//!   blocks on a fair re-acquire of the pool permit (released only as *other*
//!   scans yield), so a storage/index lock held across it could deadlock
//!   (FM-3/FM-7), mirroring the Accord `handlers.rs` "no lock across `.await`".
//!
//! Static analysis can't see these call/no-call invariants, so this test greps
//! the source — the same "guard the invariant at the source" pattern as the
//! viz-drain `truncations.push` check. It fails the build (not production) if a
//! future producer forgets to yield or holds a lock across the yield.

use std::fs;

/// Extract each range-scan producer's closure body. Producers route through the
/// `spawn_bounded_range_scan(tx, ...)` helper (which wraps the raw `submit_scan`
/// admission with cancellation + fail-loud overload); each body runs from that
/// call up to the `Box::pin(futures::stream::unfold` stream return that
/// immediately follows the producer's closure. Matching `spawn_bounded_range_scan(tx`
/// picks the call sites, not the `spawn_bounded_range_scan<F>(tx:` definition.
fn producer_bodies(src: &str) -> Vec<&str> {
    src.match_indices("spawn_bounded_range_scan(tx")
        .map(|(start, _)| {
            let rest = &src[start..];
            let end = rest
                .find("Box::pin(futures::stream::unfold")
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .collect()
}

fn store_src() -> String {
    fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/store.rs"))
        .expect("read store.rs source")
}

#[test]
fn every_submit_scan_producer_calls_slot_tick() {
    let src = store_src();
    let bodies = producer_bodies(&src);
    assert!(
        bodies.len() >= 4,
        "expected >= 4 store.rs scan producers routed through spawn_bounded_range_scan, found {} — \
         did a producer switch back to submit_blocking (no cooperative yield)?",
        bodies.len()
    );
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
    // B1 T1.5 / FM-6 — interactive point-read bypass. Two invariants keep the
    // scheduler off the point-read path:
    //   (a) `global_pool()` is called ONLY inside `spawn_bounded_range_scan`, the
    //       single helper that routes a scan through the pool; and
    //   (b) every `spawn_bounded_range_scan(...)` CALL site is a `range_iter*`
    //       scan producer.
    // Together they guarantee the point-read methods (`read` /
    // `read_limited_rows` / `read_clustering_row`) never touch the scheduler, so a
    // `PartitionKeyLookup` has zero scheduler calls. Fails if a future edit routes
    // a point read through the pool or calls the pool outside the helper.
    let src = store_src();

    let mut pool_calls = 0usize;
    for (idx, _) in src.match_indices("global_pool()") {
        let name = enclosing_fn_name(&src, idx);
        assert!(
            name == "spawn_bounded_range_scan",
            "global_pool() is called from `{name}` — the scheduler pool must be reached only \
             through spawn_bounded_range_scan. A point read must have zero scheduler calls \
             (T1.5 / FM-6)."
        );
        pool_calls += 1;
    }
    assert!(
        pool_calls >= 1,
        "expected spawn_bounded_range_scan to call global_pool(), found {pool_calls}"
    );

    let mut call_sites = 0usize;
    for (idx, _) in src.match_indices("spawn_bounded_range_scan(tx") {
        let name = enclosing_fn_name(&src, idx);
        assert!(
            name.contains("range_iter"),
            "spawn_bounded_range_scan is called from `{name}` — only range_iter* scan producers \
             may route work through the scheduler pool (T1.5 / FM-6)."
        );
        call_sites += 1;
    }
    assert!(
        call_sites >= 4,
        "expected >= 4 range scan producer call sites, found {call_sites}"
    );
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
        // `.lock()` is the clear mutex-guard signal. `tick()` blocks on a fair
        // permit re-acquire, so a guard live across it risks deadlock (T1.7).
        // The producers deliberately use arc-swap `load_full()` (owned Arcs), so
        // no guard is held; this fails if a future edit introduces one.
        assert!(
            !body.contains(".lock()"),
            "submit_scan producer #{n} acquires a `.lock()` guard inside the scan closure — \
             a lock held across slot.tick()'s blocking permit re-acquire can deadlock \
             (T1.7 / FM-3/FM-7). Load shared state via arc-swap (`load_full()`) instead, or \
             scope the guard so it is dropped before the page loop."
        );
    }
}
