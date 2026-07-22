//! B1 T1.3 source-inspection guard.
//!
//! Every `store.rs` scan producer submitted to the bounded scheduler pool via
//! `SchedPool::submit_scan` MUST cooperatively yield — i.e. call `slot.tick()`
//! in its page loop. A producer that omits `tick()` holds its pool slot for the
//! whole scan and reintroduces the monopolization B1 fixes (a long full-table
//! scan starving every other scan). Static analysis can't see the call, so this
//! test greps the source: it fails if any `submit_scan` producer lacks a
//! `slot.tick()` in its body — the same "guard the invariant at the source"
//! pattern used for the viz-drain `truncations.push` check.

use std::fs;

#[test]
fn every_submit_scan_producer_calls_slot_tick() {
    let src = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/store.rs"))
        .expect("read store.rs source");

    let producers: Vec<usize> = src.match_indices("submit_scan(").map(|(i, _)| i).collect();
    assert!(
        producers.len() >= 4,
        "expected >= 4 store.rs scan producers routed through submit_scan, found {} — \
         did a producer switch back to submit_blocking (no cooperative yield)?",
        producers.len()
    );

    // Each producer's body runs from its `submit_scan(` to the next one (or EOF).
    for (n, &start) in producers.iter().enumerate() {
        let end = producers.get(n + 1).copied().unwrap_or(src.len());
        let body = &src[start..end];
        assert!(
            body.contains("slot.tick()"),
            "the submit_scan scan producer starting at byte {start} does not call \
             slot.tick() in its page loop — a long scan would monopolize the bounded \
             pool and reintroduce the starvation B1 fixes (T1.3 / FM-2)"
        );
    }
}
