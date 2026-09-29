//! Golden sidecar corpus for the index sidecar format.
//!
//! # Why a checked-in file, when the round-trip tests already pass
//!
//! Every other sidecar test writes a sidecar and reads it back. That proves the
//! writer and the reader agree *with each other* — and nothing else. If a change
//! alters the on-disk format in both the writer and the reader, every round-trip
//! test still passes while every sidecar already on disk becomes unreadable.
//!
//! The sidecar is persisted state: it is written at flush time and read after a
//! restart, possibly by a different build. So the format needs a test that holds
//! one side fixed. `GOLDEN_POSTINGS` names the case and its recorded expected
//! walk count; the bytes live in `tests/golden/`. A format change now fails
//! against a stable artifact instead of passing against a mirror of itself.
//!
//! Regeneration lives in `sidecar_golden_regen.rs`, gated by
//! `FERROSA_REGEN_GOLDEN=1` and named to match the existing SSTable corpus
//! convention (`ferrosa-sstable/tests/golden_regen.rs`).

use std::path::PathBuf;

use ferrosa_index::{IndexKey, RowPosition};

/// The single golden case: how many postings, and the hot key every posting
/// in this fixture also carries.
///
/// Small on purpose. This test's job is format stability, not volume: 2,000
/// postings exercise the header, the offset table, the cursor and the
/// multi-page walk, and cost milliseconds to read. Volume and memory-budget
/// behaviour are `sidecar_memory_bound.rs`'s job.
pub const GOLDEN_N: usize = 2_000;

/// The key whose postings `open_and_walk` counts. Every generated partition
/// contributes exactly one posting under it, so the expected count is
/// `GOLDEN_N`.
pub const HOT_KEY: &[u8] = b"tenant-hot";

/// Every distinct-key posting's key, zero-padded to a fixed width so the
/// fixture's sort order is stable across platforms and runs.
pub fn distinct_key(i: usize) -> IndexKey {
    IndexKey(format!("tenant-{i:016}").into_bytes())
}

/// The fixture's postings, in the order the writer receives them: for each
/// `i`, one posting under `HOT_KEY` and one under its own distinct key.
///
/// This is the same shape `sidecar_memory_bound.rs` builds for its memory
/// budgets. It is deliberately a separate, frozen copy: the golden corpus must
/// not silently follow a change to another test's fixture, or it stops being a
/// fixed reference point.
pub fn golden_entries() -> Vec<(IndexKey, RowPosition)> {
    (0..GOLDEN_N)
        .flat_map(|i| {
            let partition_key = format!("tenant-partition-key-{i:016}").into_bytes();
            [
                (
                    IndexKey(HOT_KEY.to_vec()),
                    RowPosition {
                        partition_key: partition_key.clone(),
                        clustering_key: Vec::new(),
                    },
                ),
                (
                    distinct_key(i),
                    RowPosition {
                        partition_key,
                        clustering_key: Vec::new(),
                    },
                ),
            ]
        })
        .collect()
}

/// `tests/golden/` under this crate.
pub fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
}

/// The checked-in sidecar body file.
///
/// One file, because `SidecarWriter::write_streaming` removes its offset-table
/// temp file before renaming the body into place: the offset table is a build
/// artifact, not part of the persisted format. `Data.db`-style component naming
/// from the SSTable corpus does not apply here.
pub fn golden_sidecar_path() -> PathBuf {
    golden_dir().join("idx_golden.sidecar")
}

/// The checked-in manifest: the fixture's parameters plus the sidecar's
/// SHA-256 and byte length. A human-readable fingerprint that makes a format
/// change legible in review instead of an opaque binary diff.
pub fn manifest_path() -> PathBuf {
    golden_dir().join("manifest.txt")
}

/// Renders the manifest for a sidecar of `bytes`. Kept next to the writer so
/// the recorded parameters cannot drift from the file they describe.
pub fn format_manifest(sha256: &str, byte_len: usize) -> String {
    let mut out = String::new();
    out.push_str("case: idx_golden\n");
    out.push_str(&format!("postings: {}\n", GOLDEN_N * 2));
    out.push_str(&format!("hot_key_postings: {}\n", GOLDEN_N));
    out.push_str(&format!("sha256: {sha256}\n"));
    out.push_str(&format!("bytes: {byte_len}\n"));
    out.push_str("---\n");
    out
}

/// Reads the checked-in sidecar bytes. Panics with the regeneration command
/// when the corpus is absent, so a missing fixture is a loud, actionable
/// failure rather than a silently skipped test.
pub fn read_golden_sidecar() -> Vec<u8> {
    std::fs::read(golden_sidecar_path()).unwrap_or_else(|e| {
        panic!(
            "tests/golden/idx_golden.sidecar missing or unreadable ({e}); regenerate once with \
             `FERROSA_REGEN_GOLDEN=1 cargo test -p ferrosa-storage --test sidecar_golden_regen \
             -- --nocapture`"
        )
    })
}
