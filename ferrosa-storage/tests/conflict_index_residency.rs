//! Per-PR guard: the Accord `ConflictIndex` must not over-reserve per key, nor
//! store a key's bytes twice.
//!
//! This asserts only STRUCTURAL / SAME-RUN-RATIO bounds, so it holds on any
//! machine and runs in the per-PR gate:
//!
//! * the per-key writes buffer is one write wide, not four (relative to
//!   `size_of::<InFlightWrite>()`);
//! * raising the per-key HWM costs under half of what a PARALLEL
//!   key->`Timestamp` map costs, measured in the same run (the old index built
//!   exactly that parallel map, so the two matched).
//!
//! The ABSOLUTE byte-bound projection at 1,000,112 keys lives in the sibling
//! `conflict_index_residency_absolute.rs`, gated out of the per-PR lane exactly
//! like `accord::perf` (an absolute bound measures the runner). The shared,
//! measured harness is `support/residency.rs`.

#[path = "support/residency.rs"]
mod residency;

use residency::{measure, CountingAlloc};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[test]
fn conflict_index_does_not_over_reserve_or_store_key_bytes_twice() {
    let r = measure();
    println!("{}", r.report("per-PR structure/ratio guard"));

    // Factor 1 (RELATIVE): a key that holds one in-flight write must not reserve
    // room for four. One element's worth (entry_size) is the target; reject
    // anything past two, which is what the old capacity-4 buffer blows through.
    assert!(
        r.value_buf_after < 2 * r.entry_size,
        "a registered key reserved {} bytes for its writes buffer \
         (entry_size={}); the old capacity-4 Vec reserved {} — a single in-flight \
         write must not reserve room for four",
        r.value_buf_after,
        r.entry_size,
        4 * r.entry_size
    );

    // Factor 2 (SAME-RUN RATIO): the HWM must live with the key's single entry,
    // not in a parallel map that re-stores the key. The parallel stand-in — built
    // and measured in THIS run — costs `hwm_before` B/key; the new raise must cost
    // under half of that. Old: the index built exactly that parallel map, so the
    // two matched and this failed; new: raising the HWM on the existing entry
    // allocates nothing.
    assert!(
        r.hwm_after * 2 < r.hwm_before,
        "raising the per-key HWM allocated {} B/key; a parallel key->Timestamp map \
         (measured in this run) costs {} B/key — the HWM must ride the key's own \
         entry, not a second map that stores every key again",
        r.hwm_after,
        r.hwm_before
    );
}
