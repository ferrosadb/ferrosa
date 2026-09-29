//! The sidecar on-disk format is a persisted contract, so it is pinned against
//! a checked-in artifact rather than only against itself.
//!
//! `sidecar_memory_bound.rs` (and every other sidecar test) writes a sidecar
//! and reads it back. That proves the writer and the reader agree with each
//! other. It cannot prove the *format* is unchanged: a change that moved the
//! header, the offset table or the posting encoding in both the writer and the
//! reader leaves every round-trip test green while sidecars written by an
//! earlier build — or read by an earlier build — become unreadable.
//!
//! These tests hold one side fixed: the bytes in `tests/golden/`.

mod support;

use std::ops::ControlFlow;

use ferrosa_index::IndexKey;
use ferrosa_storage::index::sidecar::SidecarReader;
use support::sidecar_golden::{
    distinct_key, golden_entries, golden_sidecar_path, GOLDEN_N, HOT_KEY,
};

fn open_golden() -> SidecarReader {
    SidecarReader::open(&golden_sidecar_path()).unwrap_or_else(|e| {
        panic!(
            "opening the checked-in golden sidecar failed: {e}. A failure here means the \
             current reader cannot read sidecar bytes this project already wrote — a format \
             compatibility break, not a missing fixture. If the format change is intentional, \
             regenerate with FERROSA_REGEN_GOLDEN=1 and treat the change as a migration."
        )
    })
}

/// Every posting under the hot key is still reachable from the fixed bytes.
#[test]
fn golden_sidecar_walks_every_hot_key_posting() {
    let reader = open_golden();
    let key = IndexKey(HOT_KEY.to_vec());
    let mut walked = 0usize;
    reader
        .visit(&key, &mut |_position| {
            walked += 1;
            ControlFlow::Continue(())
        })
        .expect("visit hot key in the golden sidecar");

    assert_eq!(
        walked, GOLDEN_N,
        "the golden sidecar was written with {GOLDEN_N} postings under the hot key"
    );
}

/// A distinct key resolves to exactly its own posting — the offset table still
/// addresses the right posting after a format-stable read.
///
/// Checked at both ends and the middle: an off-by-one in the offset table shows
/// up at the boundaries first, and a mid-file check catches a stride error the
/// ends would miss.
#[test]
fn golden_sidecar_resolves_distinct_keys_at_their_own_postings() {
    let reader = open_golden();
    for i in [0usize, GOLDEN_N / 2, GOLDEN_N - 1] {
        let key = distinct_key(i);
        let mut walked = 0usize;
        reader
            .visit(&key, &mut |_position| {
                walked += 1;
                ControlFlow::Continue(())
            })
            .unwrap_or_else(|e| panic!("visit {key:?} (index {i}) in the golden sidecar: {e}"));
        assert_eq!(
            walked, 1,
            "distinct key at index {i} must resolve to exactly one posting"
        );
    }
}

/// A key that is not in the fixture walks to nothing rather than erroring or
/// picking up a neighbour's posting.
#[test]
fn golden_sidecar_has_no_posting_for_an_absent_key() {
    let reader = open_golden();
    let key = IndexKey(b"tenant-absent-from-the-corpus".to_vec());
    let mut walked = 0usize;
    reader
        .visit(&key, &mut |_position| {
            walked += 1;
            ControlFlow::Continue(())
        })
        .expect("visiting an absent key is a miss, not an error");
    assert_eq!(walked, 0, "an absent key must not match any posting");
}

/// The fixture in code still describes the corpus on disk: the entries the
/// fixture builds are exactly what a reader sees in the checked-in bytes.
///
/// This is the check that makes the corpus trustworthy as a reference. Without
/// it the golden file could drift from `golden_entries()` and the three tests
/// above would keep passing against bytes nothing generates any more.
#[test]
fn golden_sidecar_matches_the_fixture_that_generates_it() {
    let reader = open_golden();
    let entries = golden_entries();
    assert_eq!(entries.len(), GOLDEN_N * 2, "fixture shape");

    // The distinct keys are the ones a change to the generator would move.
    for i in [0usize, 1, GOLDEN_N / 2, GOLDEN_N - 1] {
        let key = distinct_key(i);
        let mut hits = Vec::new();
        reader
            .visit(&key, &mut |position| {
                hits.push(position);
                ControlFlow::Continue(())
            })
            .unwrap_or_else(|e| panic!("visit {key:?}: {e}"));
        assert_eq!(
            hits.len(),
            1,
            "fixture key {key:?} must be present exactly once in the golden corpus"
        );
        let expected = entries
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, p)| p)
            .expect("fixture builds this key");
        assert_eq!(
            hits[0].partition_key, expected.partition_key,
            "the golden corpus's posting for {key:?} differs from what the fixture builds"
        );
    }
}
