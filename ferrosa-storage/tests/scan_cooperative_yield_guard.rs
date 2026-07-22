//! B1 T1.3 + T1.7 source-inspection guards for the cooperative scan producers.
//!
//! Every `store.rs` scan producer submitted to the bounded scheduler pool via
//! `SchedPool::submit_scan` must:
//!
//! * **T1.3** — call `slot.tick()` in its page loop, or a long scan holds its
//!   pool slot for the whole scan and reintroduces the monopolization B1 fixes.
//! * **T1.7** — hold **no lock across `slot.tick()`**. `tick()` blocks on a
//!   fair re-acquire of the pool permit (released only as *other* scans yield),
//!   so a storage/index lock held across it could deadlock (FM-3/FM-7), mirroring
//!   the Accord `handlers.rs` "no lock across `.await`" contract.
//!
//! Static analysis can't see these call/no-call invariants, so this test greps
//! the source — the same "guard the invariant at the source" pattern as the
//! viz-drain `truncations.push` check. It fails the build (not production) if a
//! future producer forgets to yield or holds a lock across the yield.

use std::fs;

/// Extract each `submit_scan` producer's closure body — from `submit_scan(` up
/// to the `Box::pin(futures::stream::unfold` stream return that immediately
/// follows every producer's closure. Bounds the body precisely so post-closure
/// code isn't mis-attributed.
fn producer_bodies(src: &str) -> Vec<&str> {
    src.match_indices("submit_scan(")
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
        "expected >= 4 store.rs scan producers routed through submit_scan, found {} — \
         did a producer switch back to submit_blocking (no cooperative yield)?",
        bodies.len()
    );
    for (n, body) in bodies.iter().enumerate() {
        assert!(
            body.contains("slot.tick()"),
            "submit_scan producer #{n} does not call slot.tick() in its page loop — a long \
             scan would monopolize the bounded pool and reintroduce the starvation B1 fixes \
             (T1.3 / FM-2)"
        );
    }
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
