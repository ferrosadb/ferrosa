//! Regenerates the golden index-sidecar corpus.
//!
//! Destructive (overwrites `tests/golden/`) and **never runs in normal CI**:
//! the rewrite only happens when `FERROSA_REGEN_GOLDEN` is set. Without it,
//! this test still performs a real check — that the checked-in manifest and
//! the checked-in sidecar agree with each other and with today's fixture
//! parameters — so `cargo test` never silently no-ops here; it just doesn't
//! touch the corpus.
//!
//! Regenerate with:
//!   FERROSA_REGEN_GOLDEN=1 cargo test -p ferrosa-storage --test sidecar_golden_regen -- --nocapture
//!
//! Regenerate only when the on-disk format change is INTENTIONAL. A diff here
//! in an unrelated PR means the format moved and every sidecar already written
//! by an older build is now a compatibility question.

mod support;

use ferrosa_index::IndexKey;
use ferrosa_storage::index::sidecar::{SidecarReader, SidecarWriter};
use sha2::{Digest, Sha256};
use support::sidecar_golden::{
    format_manifest, golden_entries, golden_sidecar_path, manifest_path, read_golden_sidecar,
    GOLDEN_N, HOT_KEY,
};

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[test]
fn golden_regen_writes_sidecar_when_requested() {
    if std::env::var("FERROSA_REGEN_GOLDEN").is_ok() {
        regenerate();
    }

    // Always-on, whether or not regeneration ran: the manifest on disk
    // describes the sidecar on disk. Both are checked in, so a format change
    // that updated one and not the other fails here.
    let manifest = std::fs::read_to_string(manifest_path()).unwrap_or_else(|e| {
        panic!(
            "tests/golden/manifest.txt missing or unreadable ({e}); run \
             `FERROSA_REGEN_GOLDEN=1 cargo test -p ferrosa-storage --test sidecar_golden_regen \
             -- --nocapture` once"
        )
    });
    let sidecar = read_golden_sidecar();

    let recorded_sha = manifest
        .lines()
        .find_map(|line| line.strip_prefix("sha256: "))
        .expect("manifest must record sha256");
    let recorded_bytes: usize = manifest
        .lines()
        .find_map(|line| line.strip_prefix("bytes: "))
        .and_then(|v| v.parse().ok())
        .expect("manifest must record bytes");
    let recorded_hot: usize = manifest
        .lines()
        .find_map(|line| line.strip_prefix("hot_key_postings: "))
        .and_then(|v| v.parse().ok())
        .expect("manifest must record hot_key_postings");

    assert_eq!(
        recorded_hot, GOLDEN_N,
        "the manifest records a different hot-key posting count than the fixture builds"
    );
    assert_eq!(
        recorded_bytes,
        sidecar.len(),
        "manifest says {recorded_bytes} bytes but tests/golden/idx_golden.sidecar is {}; \
         regenerate both together",
        sidecar.len()
    );
    assert_eq!(
        recorded_sha,
        sha256_hex(&sidecar),
        "the checked-in sidecar does not match its manifest sha256; regenerate both together"
    );
}

/// Rebuilds the corpus from the fixture and writes it, plus the manifest.
fn regenerate() {
    let dir = golden_sidecar_path();
    let path = dir.as_path();
    std::fs::create_dir_all(path.parent().expect("golden dir"))
        .unwrap_or_else(|e| panic!("mkdir {}: {e}", path.parent().unwrap().display()));

    let entries = golden_entries();
    SidecarWriter::write(path, &entries).expect("write golden sidecar");

    // `write` sorts and dedups before writing, so the file's posting count is
    // derived from what the reader sees, not from the input length. Assert on
    // the read side so a dedup that collapsed postings the fixture intended to
    // be distinct cannot slip through as a smaller-but-consistent corpus.
    let read_back = SidecarReader::open(path).expect("open the sidecar just written");
    let key = IndexKey(HOT_KEY.to_vec());
    let mut hot_postings = 0usize;
    read_back
        .visit(&key, &mut |_| {
            hot_postings += 1;
            std::ops::ControlFlow::Continue(())
        })
        .expect("count hot-key postings in the fresh sidecar");
    assert_eq!(
        hot_postings, GOLDEN_N,
        "the golden fixture must write {GOLDEN_N} distinct hot-key postings; a smaller count \
         means the partition keys were not distinct and the corpus is not what the manifest says"
    );

    let bytes = std::fs::read(path).expect("read back the sidecar just written");
    std::fs::write(
        manifest_path(),
        format_manifest(&sha256_hex(&bytes), bytes.len()),
    )
    .expect("write golden manifest");

    println!(
        "regenerated {} ({} bytes, {} hot-key postings) and {}",
        path.display(),
        bytes.len(),
        hot_postings,
        manifest_path().display()
    );
}
