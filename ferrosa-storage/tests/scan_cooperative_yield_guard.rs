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

/// Extract each range-scan producer's body. Two shapes exist:
///
/// - fragment producers route a closure through the
///   `spawn_bounded_range_scan(tx, ...)` helper (which wraps the raw
///   `submit_scan` admission with cancellation + fail-loud overload); each body
///   runs from that call up to the `Box::pin(futures::stream::unfold` stream
///   return that immediately follows the closure. Matching
///   `spawn_bounded_range_scan(tx` picks the call sites, not the
///   `spawn_bounded_range_scan<F>(tx:` definition;
/// - the whole-partition producer is `RangeScan::run`, run (and re-run after
///   each pause) by `spawn_resumable_range_scan`.
fn producer_bodies(src: &str) -> Vec<&str> {
    let mut bodies: Vec<&str> = src
        .match_indices("spawn_bounded_range_scan(tx")
        .map(|(start, _)| {
            let rest = &src[start..];
            let end = rest
                .find("Box::pin(futures::stream::unfold")
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .collect();
    bodies.push(range_scan_run_body(src));
    bodies
}

/// The body of `RangeScan::run`: from its declaration to the next item.
fn range_scan_run_body(src: &str) -> &str {
    let impl_at = src
        .find("impl<F: FlushTarget> RangeScan<F>")
        .expect("RangeScan's impl must exist");
    let rest = &src[impl_at..];
    let run_at = rest.find("fn run(").expect("RangeScan::run must exist");
    let body = &rest[run_at..];
    let end = body.find("\nfn ").unwrap_or(body.len());
    &body[..end]
}

/// Functions that may call `global_pool()`: the two helpers that route a range
/// scan through the scheduler.
const POOL_HELPERS: [&str; 2] = ["spawn_bounded_range_scan", "spawn_resumable_range_scan"];

fn store_src() -> String {
    fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/store.rs"))
        .expect("read store.rs source")
}

#[test]
fn every_submit_scan_producer_calls_slot_tick() {
    let src = store_src();
    let bodies = producer_bodies(&src);
    assert!(
        bodies.len() >= 3,
        "expected >= 3 store.rs scan producers (two fragment producers and RangeScan::run), \
         found {} — did a producer switch back to submit_blocking (no cooperative yield)?",
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
    // B1 T1.5 / FM-6 — interactive point-read bypass. Three invariants keep the
    // scheduler off the point-read path:
    //   (a) `global_pool()` is called ONLY inside the two helpers that route a
    //       scan through the pool (`POOL_HELPERS`);
    //   (b) every `spawn_bounded_range_scan(...)` CALL site is a `range_iter*`
    //       scan producer, and `spawn_resumable_range_scan(...)` is called only
    //       from `whole_partition_range_scan`; and
    //   (c) `whole_partition_range_scan` is called only from `range_iter*`.
    // Together they guarantee the point-read methods (`read` /
    // `read_limited_rows` / `read_clustering_row`) never touch the scheduler, so a
    // `PartitionKeyLookup` has zero scheduler calls. Fails if a future edit routes
    // a point read through the pool or calls the pool outside the helpers.
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
        pool_calls >= 2,
        "expected both {POOL_HELPERS:?} to call global_pool(), found {pool_calls}"
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
    for (idx, _) in src.match_indices("spawn_resumable_range_scan(tx") {
        let name = enclosing_fn_name(&src, idx);
        assert_eq!(
            name, "whole_partition_range_scan",
            "spawn_resumable_range_scan is called from `{name}` (T1.5 / FM-6)"
        );
        call_sites += 1;
    }
    for (idx, _) in src.match_indices("self.whole_partition_range_scan(") {
        let name = enclosing_fn_name(&src, idx);
        assert!(
            name.contains("range_iter"),
            "whole_partition_range_scan is called from `{name}` — only range_iter* scan \
             producers may route work through the scheduler pool (T1.5 / FM-6)."
        );
    }
    assert!(
        call_sites >= 3,
        "expected >= 3 range scan producer call sites, found {call_sites}"
    );
}

/// A whole-partition scan whose consumer stopped reading (a suspended PG
/// portal, a client that left its socket full) must give back its thread, not
/// only its slot: parked threads add up until the runtime's bounded blocking
/// pool is gone (missing-guards entry 8). So `RangeScan::run` sends only
/// through `deliver_or_pause`/`deliver_failure`, never the `deliver` that
/// parks for as long as the consumer likes.
#[test]
fn the_whole_partition_producer_pauses_instead_of_parking() {
    let src = store_src();
    let run = range_scan_run_body(&src);
    assert!(
        run.contains("deliver_or_pause("),
        "RangeScan::run must send through deliver_or_pause"
    );
    for parking in ["deliver(", "deliver_error(", "slot.park("] {
        assert!(
            !run.contains(parking),
            "RangeScan::run calls `{parking}`, which keeps the thread while the consumer \
             is not reading — pause with deliver_or_pause/deliver_failure instead"
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

/// A producer that blocks on its consumer while holding a pool slot lets a
/// client that stopped reading stall every other scan on the node
/// (`pg_stalled_consumer_liveness`). Every send must go through `deliver`,
/// which parks the slot while the send waits.
#[test]
fn no_producer_blocks_on_its_consumer_while_holding_a_slot() {
    let src = store_src();
    for (n, body) in producer_bodies(&src).iter().enumerate() {
        assert!(
            !body.contains("blocking_send"),
            "range-scan producer #{n} calls blocking_send directly — it would hold its pool \
             slot while a client that stopped reading leaves the send pending. Send through \
             deliver()/deliver_error(), which park the slot."
        );
        assert!(
            body.contains("deliver(") || body.contains("deliver_or_pause("),
            "range-scan producer #{n} never sends through deliver()/deliver_or_pause()"
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
